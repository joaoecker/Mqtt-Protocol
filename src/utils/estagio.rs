use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crate::utils::udp;
use crate::utils::vini::ConfigIni;
use crate::Cache;

static COUNTER: AtomicU32 = AtomicU32::new(0);

pub async fn process_estagio(
    timestamp: i64,
    machine_id: String,
    machine_id_full: String,
    estagio: u8,
    udp_host: &str,
    udp_port: u16,
    topico: &str,
    mut cache_estado_topico: Cache<u32>,
) -> () {
    let config = ConfigIni::new();
    match estagio {
        0 | 1 => {
            let packet = format!("SP,{},{},1,0,R,EP", timestamp, machine_id);
            udp::send_udp(&packet, udp_host, udp_port).await;
        }
        2 => {
            let packet = format!("SP,{},{},1,1,R,EP", timestamp, machine_id);
            udp::send_udp(&packet, udp_host, udp_port).await;
        }
        3 => {
            if cache_estado_topico
                .get(topico.to_owned())
                .await
                .unwrap_or(0)
                != 3
            {
                let count_on = COUNTER.fetch_add(1, Ordering::Relaxed);
                if count_on <= 10_000 {
                    let packet_on = format!("SC,{},{},1,1,{},0", timestamp, machine_id, count_on);
                    udp::send_udp(&packet_on, udp_host, udp_port).await;
                    if machine_id_full != "0000" {
                        let packet_port_2 = format!("EE,{},'',{}", machine_id_full, timestamp);
                        udp::send_udp(&packet_port_2, udp_host, config.udp_port_2).await;
                    }
                    let machine_id_clone = machine_id.clone();
                    let udp_host_clone = udp_host.to_string();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        let count_off = COUNTER.fetch_add(1, Ordering::Relaxed);
                        let packet_off =
                            format!("SC,{},{},1,0,{},0", timestamp, machine_id_clone, count_off);
                        udp::send_udp(&packet_off, &udp_host_clone, udp_port).await;
                    });
                }
            }
        }
        _ => {}
    };
    cache_estado_topico
        .set(topico.to_owned(), estagio.into())
        .await;
}
