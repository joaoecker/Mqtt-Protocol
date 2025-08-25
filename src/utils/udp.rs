use tokio::net::UdpSocket;

pub async fn send_udp(packet: &str, host: &str, port: u16) {
    println!("Enviando UDP: {}", packet);
    if let Ok(socket) = UdpSocket::bind("0.0.0.0:0").await {
        let _ = socket
            .send_to(packet.as_bytes(), format!("{}:{}", host, port))
            .await;
    }
}
