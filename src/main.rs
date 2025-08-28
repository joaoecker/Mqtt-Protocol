use chrono::Local;
use rsfbclient::Queryable;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockWriteGuard};
use tokio::time;

mod utils;
use mqtt::MachineState;
use utils::vini::ConfigIni;
use utils::{db, estagio, logger, mqtt, udp};

#[derive(Clone)]
pub struct Cache<T: Clone> {
    inner: Arc<RwLock<HashMap<String, T>>>,
}

impl<T: Clone> Cache<T> {
    pub fn new() -> Self {
        Cache {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get(&mut self, index: String) -> Option<T> {
        let guard = self.inner.read().await;
        guard.get(&index).map(|v| v.clone())
    }

    pub async fn set(&mut self, index: String, value: T) -> Option<T> {
        let mut guard = self.inner.write().await;
        guard.insert(index, value)
    }

    pub async fn write(&mut self) -> RwLockWriteGuard<'_, HashMap<String, T>> {
        self.inner.write().await
    }
}

#[tokio::main]
async fn main() {
    let config = ConfigIni::new();
    let db_pool = db::initiate_firebird_connection(&config);
    let mut cache_estado_topico: Cache<u32> = Cache::new();
    let topic_to_id: Arc<HashMap<String, String>> = match db_pool.get() {
    Ok(mut conn) => {
        match conn.query::<(), (String, String)>(
            "SELECT U.ID_MAQUINAS, U.IP FROM UDP_CONFIG U INNER JOIN MAQUINAS M ON U.ID_MAQUINAS = M.ID_MAQUINAS WHERE U.ATIVO = '3'",
            (),
        ) {
            Ok(result) => {
                let map: HashMap<String, String> = result
                    .into_iter()
                    .map(|(id_maquina, topico)| (topico, id_maquina))
                    .collect();
                println!("Máquinas ativas no banco:");
                for (topico, id) in &map {
                    cache_estado_topico.set(topico.clone(), 0).await;
                    println!("{} -> {}", topico, id);
                }
                Arc::new(map)
            }
            Err(e) => {
                eprintln!("Erro na query: {}", e);
                Arc::new(HashMap::new())
            }
        }
    }
    Err(e) => {
        eprintln!("Erro ao obter conexão da pool: {}", e);
        Arc::new(HashMap::new())
    }
};

    let states_map = Arc::new(RwLock::new(HashMap::<String, MachineState>::new()));
    let mut mqttoptions = MqttOptions::new("rust-client", &config.mqtt_broker, config.mqtt_port);
    mqttoptions.set_keep_alive(std::time::Duration::from_secs(30));
    mqttoptions.set_clean_session(false);
    mqttoptions.set_credentials(&config.user, &config.pass);

    let (client, eventloop) = AsyncClient::new(mqttoptions, 10);
    println!("Broker conectado");

    for topic in topic_to_id.keys() {
        client
            .subscribe(topic, QoS::AtLeastOnce)
            .await
            .unwrap_or_else(|e| eprintln!("Erro ao subscrever no tópico {}: {}", topic, e));
    }

    let topic_to_id_mqtt = Arc::clone(&topic_to_id);
    let states_map_mqtt = Arc::clone(&states_map);
    let udp_host = config.udp_host.clone();
    let udp_port = config.udp_port;
    tokio::spawn(async move {
        mqtt::start_mqtt(
            client,
            eventloop,
            topic_to_id_mqtt,
            states_map_mqtt,
            udp_host,
            udp_port,
            cache_estado_topico,
        )
        .await;
    });

    let states_map_udp = Arc::clone(&states_map);
    let udp_host = config.udp_host.clone();
    let udp_port = config.udp_port;
    tokio::spawn(async move {
        let udpsocket = tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();
        udpsocket
            .connect(format!("{}:{}", udp_host, udp_port))
            .await
            .unwrap();

        loop {
            let mut map = states_map_udp.write().await;
            let now = Local::now().timestamp();

            for (machine_id, machine_state) in map.iter_mut() {
                if now - machine_state.last_sent >= 10 {
                    let packet = format!(
                        "SP,{},{},1,{},R,EP",
                        machine_state.timestamp, machine_id, machine_state.state
                    );
                    println!("Reenviando pacote periódico: {}", packet);

                    if let Err(e) = udpsocket.send(packet.as_bytes()).await {
                        eprintln!("Erro ao enviar via UDP: {}", e);
                    } else {
                        machine_state.last_sent = now;
                    }
                }
            }
            time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    loop {
        // time::sleep(std::time::Duration::from_secs(config.time_cicle_ms / 1000)).await;
    }
}
