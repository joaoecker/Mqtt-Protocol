use crate::utils::estagio;
use crate::utils::logger;
use crate::utils::udp;
use crate::Cache;
use chrono::Local;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Deserialize)]
pub struct MqttData {
    #[serde(rename = "ESTAGIO")]
    pub estagio: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MqttPayload {
    pub d: Option<MqttData>,
}

#[derive(Debug, Clone)]
pub struct MachineState {
    pub state: u8,
    pub timestamp: i64,
    pub last_sent: i64,
}

#[derive(Debug, Clone)]
pub struct MachineMessage {
    pub machine_id: String,
    pub state: u8,
    pub timestamp: i64,
}

pub async fn start_mqtt<'a>(
    client: AsyncClient,
    mut eventloop: rumqttc::EventLoop,
    topic_to_id: Arc<HashMap<String, String>>,
    states_map: Arc<RwLock<HashMap<String, MachineState>>>,
    udp_host: String,
    udp_port: u16,
    cache_estado_topico: Cache<u32>,
) {
    loop {
        match eventloop.poll().await {
            Ok(notification) => {
                let Event::Incoming(Packet::Publish(publish)) = dbg!(notification) else {
                    continue;
                };

                let payload_str = String::from_utf8_lossy(&publish.payload).to_string();
                let topic = publish.topic.clone();
                println!("Recebido: {}:{}", topic, payload_str);

                if let Err(e) = process_mqtt_message(
                    &topic,
                    &payload_str,
                    &topic_to_id,
                    &states_map,
                    &udp_host,
                    udp_port,
                    cache_estado_topico.clone(),
                )
                .await
                {
                    eprintln!("Erro ao processar mensagem: {}", e);
                }

                let _ = logger::save_log(&topic, &payload_str).await;
            }
            Err(e) => {
                eprintln!("Erro no loop MQTT: {:?}", e);
                let _ = logger::save_log("MQTT_LOOP_ERROR", &format!("{:?}", e)).await;
            }
        }
    }
}

async fn process_mqtt_message(
    topic: &str,
    payload_str: &str,
    topic_to_id: &HashMap<String, String>,
    states_map: &RwLock<HashMap<String, MachineState>>,
    udp_host: &str,
    udp_port: u16,
    cache_estado_topico: crate::Cache<u32>,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload: MqttPayload = serde_json::from_str(payload_str)?;

    let Some(data) = payload.d else { return Ok(()) };
    let Some(estagio_str) = data.estagio else {
        return Ok(());
    };
    let estagio_num: u8 = estagio_str.parse()?;

    let machine_id_full = topic_to_id.get(topic).cloned().unwrap_or_else(|| {
        eprintln!("Tópico não encontrado no mapa: {}", topic);
        "0000".to_string()
    });

    let machine_id = machine_id_full.trim_start_matches('0').to_string();

    let timestamp = Local::now().timestamp();
    let msg = MachineMessage {
        machine_id: machine_id.clone(),
        state: if estagio_num == 0 { 0 } else { 1 },
        timestamp,
    };

    states_map.write().await.insert(
        msg.machine_id.clone(),
        MachineState {
            state: msg.state,
            timestamp: msg.timestamp,
            last_sent: msg.timestamp,
        },
    );

    estagio::process_estagio(
        timestamp,
        machine_id,
        machine_id_full,
        estagio_num,
        udp_host,
        udp_port,
        topic,
        cache_estado_topico.clone(),
    )
    .await;

    Ok(())
}
