use chrono::Local;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

pub async fn save_log(topic: &str, payload: &str) -> std::io::Result<()> {
    let today = Local::now().format("%Y-%m-%d").to_string();
    let filename = format!("mqtt_log_{}.txt", today);

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(filename)
        .await?;

    let now = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let log_line = format!("{} | {} | {}\n", now, topic, payload);

    file.write_all(log_line.as_bytes()).await?;
    Ok(())
}
