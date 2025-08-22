use chrono::Local;
use r2d2::Pool;
use r2d2_firebird::FirebirdConnectionManager;
use rsfbclient::{charset::WIN_1252, PureRustConnectionBuilder, Queryable};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::{
    fs::OpenOptions,
    io::AsyncWriteExt,
    sync::{mpsc, RwLock},
    time,
};

mod utils;
use utils::vini::ConfigIni;

#[derive(Debug, Clone)]
struct MachineState {
    state: u8,
    timestamp: i64,
}

#[derive(Debug, Clone)]
struct MachineMessage {
    machine_id: String,
    state: u8,
    timestamp: i64,
}

impl From<String> for MachineMessage {
    fn from(raw_message: String) -> Self {
        let parts: Vec<&str> = raw_message.splitn(2, ':').collect();
        let (topic, state_str) = match parts.as_slice() {
            [topic, state] => (topic.trim(), state.trim().to_lowercase()),
            [topic] => (topic.trim(), "off".to_string()),
            _ => ("unknown", "off".to_string()),
        };

        let machine_id = match topic {
            "T415IHMPARAM" => "1",
            "Apont/450T" => "2",
            "Apont/650T" => "3",
            _ => {
                eprintln!("Tópico desconhecido: {}", topic);
                "unknown"
            }
        }
        .to_string();

        let state = match state_str.as_str() {
            "on" => 1,
            "off" => 0,
            _ => {
                eprintln!("Estado inválido: '{}', usando 'off'", state_str);
                0
            }
        };

        MachineMessage {
            machine_id,
            state,
            timestamp: Local::now().timestamp(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct MqttData {
    #[serde(rename = "ESTAGIO")]
    estagio: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MqttPayload {
    d: Option<MqttData>,
}

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn initiate_firebird_connection(
    config: &ConfigIni,
) -> Arc<Pool<FirebirdConnectionManager<PureRustConnectionBuilder>>> {
    let connection_builder = {
        let mut connection_builder = rsfbclient::builder_pure_rust();

        connection_builder
            .host("localhost")
            .port(3050)
            .db_name(&config.db_path)
            .user("SYSDBA")
            .pass("masterkey")
            .charset(WIN_1252);
        connection_builder
    };
    let manager = FirebirdConnectionManager::new(connection_builder);
    Arc::new(
        Pool::builder()
            .max_size(100)
            .min_idle(Some(10))
            .idle_timeout(Some(Duration::new(10, 0)))
            .build(manager)
            .expect("Erro ao criar pool Firebird"),
    )
}

#[tokio::main]
async fn main() {
    let config = ConfigIni::new();
    let db_pool = initiate_firebird_connection(&config);

    match db_pool.get() {
        Ok(mut conn) => {
            match conn.query::<(), (String,)>(
                "SELECT U.ID_MAQUINAS FROM UDP_CONFIG U INNER JOIN MAQUINAS M ON U.ID_MAQUINAS = M.ID_MAQUINAS WHERE U.ATIVO = '3'",
                (),
            ) {
                Ok(result) => {
                    println!("Máquinas ativas no banco:");
                    for (id_maquina,) in result {
                        println!("{}", id_maquina);
                    }
                }
                Err(e) => eprintln!("Erro na query: {}", e),
            }
        }
        Err(e) => eprintln!("Erro ao obter conexão da pool: {}", e),
    }

    let states_map = Arc::new(RwLock::new(HashMap::<String, MachineState>::new()));
    let mut mqttoptions = MqttOptions::new("rust-client", &config.mqtt_broker, config.mqtt_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_clean_session(false);
    mqttoptions.set_credentials(&config.user, &config.pass);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    println!("Broker conectado");

    for (_, machine) in &config.machines {
        client
            .subscribe(&machine.channel, QoS::AtLeastOnce)
            .await
            .unwrap_or_else(|e| {
                eprintln!("Erro ao subscrever no tópico {}: {}", machine.channel, e)
            });
    }

    let states_map_mqtt = Arc::clone(&states_map);
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(notification) => {
                    if let Event::Incoming(Packet::Publish(publish)) = notification {
                        let payload_str = String::from_utf8_lossy(&publish.payload).to_string();
                        let topic = publish.topic.clone();
                        println!("Recebido: {}:{}", topic, payload_str);

                        match serde_json::from_str::<MqttPayload>(&payload_str) {
                            Ok(payload) => {
                                // Verifica se a mensagem tem o campo ESTAGIO
                                let Some(data) = payload.d else {
                                    println!("Mensagem sem campo 'd' recebida, ignorando");
                                    continue;
                                };

                                let Some(estagio_str) = data.estagio else {
                                    println!("Mensagem sem ESTAGIO recebida, ignorando");
                                    continue;
                                };

                                let Ok(estagio) = estagio_str.parse::<u8>() else {
                                    println!(
                                        "Valor de ESTAGIO inválido: '{}', ignorando",
                                        estagio_str
                                    );
                                    continue;
                                };

                                let msg: MachineMessage = format!(
                                    "{}:{}",
                                    topic,
                                    if estagio == 0 { "off" } else { "on" }
                                )
                                .into();

                                states_map_mqtt.write().await.insert(
                                    msg.machine_id.clone(),
                                    MachineState {
                                        state: msg.state,
                                        timestamp: msg.timestamp,
                                    },
                                );
                                println!(
                                    "Atualizado estado da máquina {} para {} com timestamp {} (Estágio: {})",
                                    msg.machine_id, msg.state, msg.timestamp, estagio
                                );

                                if estagio == 2 {
                                    let packet =
                                        format!("SP,{},{},1,1,R,EP", msg.timestamp, msg.machine_id);
                                    println!("Enviando SP: {}", packet);
                                    send_udp(&packet).await;
                                }

                                if estagio == 3 {
                                    let count_on = COUNTER.fetch_add(1, Ordering::Relaxed);
                                    if count_on <= 10_000 {
                                        let machine_id = msg.machine_id.clone();
                                        let packet_on = format!(
                                            "SC,{},{},1,1,{},0",
                                            msg.timestamp, machine_id, count_on
                                        );
                                        println!("Enviando SC ON: {}", packet_on);
                                        send_udp(&packet_on).await;

                                        tokio::spawn(async move {
                                            time::sleep(Duration::from_secs(3)).await;
                                            let count_off = COUNTER.fetch_add(1, Ordering::Relaxed);
                                            let packet_off = format!(
                                                "SC,{},{},1,0,{},0",
                                                msg.timestamp, machine_id, count_off
                                            );
                                            println!("Enviando SC OFF: {}", packet_off);
                                            send_udp(&packet_off).await;
                                        });
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!(
                                    "Erro ao parsear JSON: {} - Conteúdo: {}",
                                    e, payload_str
                                );
                            }
                        }

                        if let Err(e) = save_log(&topic, &payload_str).await {
                            eprintln!("Erro ao salvar log: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Erro no loop MQTT: {:?}", e);
                    break;
                }
            }
        }
    });

    let states_map_udp = Arc::clone(&states_map);
    tokio::spawn(async move {
        let udpsocket = tokio::net::UdpSocket::bind("localhost:1000").await.unwrap();
        udpsocket.connect("localhost:1000").await.unwrap();
        loop {
            {
                let map = states_map_udp.read().await;
                for (machine_id, machine_state) in map.iter() {
                    let packet = format!(
                        "SP,{},{},1,{},R,EP",
                        machine_state.timestamp, machine_id, machine_state.state
                    );
                    println!("Enviando pacote: {}", packet);

                    if let Err(e) = udpsocket.send(packet.as_bytes()).await {
                        eprintln!("Erro ao enviar via UDP: {}", e);
                    }
                }
            }
            time::sleep(Duration::from_secs(1)).await;
        }
    });

    loop {
        time::sleep(Duration::from_secs(config.time_cicle_ms / 1000)).await;
    }
}

async fn send_udp(packet: &str) {
    if let Ok(socket) = tokio::net::UdpSocket::bind("0.0.0.0:0").await {
        if let Err(e) = socket.send_to(packet.as_bytes(), "localhost:1000").await {
            eprintln!("Erro ao enviar UDP: {}", e);
        }
    }
}

async fn save_log(topic: &str, payload: &str) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("mqtt_log.txt")
        .await?;

    let now = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let log_line = format!("{} | {} | {}\n", now, topic, payload);
    file.write_all(log_line.as_bytes()).await?;
    Ok(())
}
