use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::time;

use crate::utils::udp;

static COUNTER: AtomicU32 = AtomicU32::new(0);

pub async fn process_estagio(
    timestamp: i64,
    machine_id: String,
    estagio: u8,
    udp_host: &str,
    udp_port: u16,
) {
    if estagio == 0 || estagio == 1 {
        let packet = format!("SP,{},{},1,0,R,EP", timestamp, machine_id);
        udp::send_udp(&packet, udp_host, udp_port).await;
    } else if estagio == 2 {
        let packet = format!("SP,{},{},1,1,R,EP", timestamp, machine_id);
        udp::send_udp(&packet, udp_host, udp_port).await;
    } else if estagio == 3 {
        let count_on = COUNTER.fetch_add(1, Ordering::Relaxed);
        if count_on <= 10_000 {
            let packet_on = format!("SC,{},{},1,1,{},0", timestamp, machine_id, count_on);
            udp::send_udp(&packet_on, udp_host, udp_port).await;

            let machine_id_clone = machine_id.clone();
            let udp_host_clone = udp_host.to_string();
            tokio::spawn(async move {
                time::sleep(Duration::from_secs(3)).await;
                let count_off = COUNTER.fetch_add(1, Ordering::Relaxed);
                let packet_off =
                    format!("SC,{},{},1,0,{},0", timestamp, machine_id_clone, count_off);
                udp::send_udp(&packet_off, &udp_host_clone, udp_port).await;
            });
        }
    }
}
