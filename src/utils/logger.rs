use chrono::Local;
use serde_json::Value;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;

pub async fn save_log(topic: &str, payload: &str) -> std::io::Result<()> {
    let today = Local::now().format("%Y-%m-%d").to_string();
    let filename = format!("mqtt_log_{}.txt", today);

    let estagio: String = match serde_json::from_str::<Value>(payload) {
        Ok(json) => json
            .get("d")
            .and_then(|d| d.get("ESTAGIO"))
            .and_then(|e| {
                if let Some(s) = e.as_str() {
                    Some(s.to_string())
                } else if let Some(n) = e.as_i64() {
                    Some(n.to_string())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| "N/A".to_string()),
        Err(_) => "N/A".to_string(),
    };

    let now = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let log_line = format!("{} | {} | ESTAGIO: {}\n", now, topic, estagio);

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(filename)
        .await?;

    file.write_all(log_line.as_bytes()).await?;
    Ok(())
}
