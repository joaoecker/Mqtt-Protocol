#![allow(dead_code)]

use chrono::NaiveTime;
use ini::Ini;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct MachineConfig {
    pub id: String,
    pub channel: String,
}

#[derive(Debug, Clone)]
pub struct ConfigIni {
    pub db_path: String,
    pub mqtt_broker: String,
    pub mqtt_port: u16,
    pub mqtt_topic: String,
    pub machine_ids: String,
    pub canal: u16,
    pub time_cicle_ms: u64,
    pub log_path: String,
    pub udp_host: String,
    pub udp_port: u16,
    pub udp_port_2: u16,
    pub user: String,
    pub pass: String,
    pub start_time: NaiveTime,
    pub end_time: NaiveTime,
    pub machines: HashMap<String, MachineConfig>,
    pub mqtt_clean_session: bool,
    pub mqtt_keep_alive: u64,
}

impl Default for ConfigIni {
    fn default() -> Self {
        ConfigIni {
            db_path: String::default(),
            mqtt_broker: String::default(),
            mqtt_topic: String::from("maq"),
            mqtt_port: 1883,
            machine_ids: String::default(),
            canal: 0,
            user: String::default(),
            pass: String::default(),
            time_cicle_ms: 10000,
            log_path: String::default(),
            udp_host: String::from("127.0.0.1"),
            udp_port: 1000,
            udp_port_2: 1005,
            start_time: NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
            end_time: NaiveTime::from_hms_opt(18, 0, 0).unwrap(),
            machines: HashMap::default(),
            mqtt_clean_session: true,
            mqtt_keep_alive: 30,
        }
    }
}

impl ConfigIni {
    pub fn new() -> ConfigIni {
        let mut config = ConfigIni::default();

        let ini = Ini::load_from_file("config.ini").expect("Erro ao carregar config.ini");

        for (section, props) in ini.iter() {
            match section {
                Some("DEFAULT") | None => {
                    for (key, value) in props.iter() {
                        match key {
                            "db_path" => config.db_path = value.to_string(),
                            "mqtt_broker" => config.mqtt_broker = value.to_string(),
                            "mqtt_topic" => config.mqtt_topic = value.to_string(),
                            "mqtt_port" => config.mqtt_port = value.parse().unwrap_or(1883),
                            "machine_ids" => config.machine_ids = value.to_string(),
                            "canal" => config.canal = value.parse().unwrap_or(0),
                            "user" => config.user = value.to_string(),
                            "pass" => config.pass = value.to_string(),
                            "timecicle" => config.time_cicle_ms = value.parse().unwrap_or(10000),
                            "log_path" => config.log_path = value.to_string(),
                            "udp_host" => config.udp_host = value.to_string(),
                            "udp_port" => config.udp_port = value.parse().unwrap_or(1000),
                            "udp_port_2" => config.udp_port_2 = value.parse().unwrap_or(1005),
                            "start_time" => {
                                config.start_time =
                                    NaiveTime::parse_from_str(value, "%H:%M:%S").unwrap()
                            }
                            "end_time" => {
                                config.end_time =
                                    NaiveTime::parse_from_str(value, "%H:%M:%S").unwrap()
                            }
                            "mqtt_clean_session" => {
                                config.mqtt_clean_session = value.parse::<bool>().unwrap_or(true)
                            }
                            "mqtt_keep_alive" => {
                                config.mqtt_keep_alive = value.parse::<u64>().unwrap_or(30)
                            }
                            _ => {}
                        }
                    }
                }
                Some(s) if s.starts_with("machine_") => {
                    if let (Some(id), Some(channel)) = (props.get("id"), props.get("channel")) {
                        config.machines.insert(
                            id.to_string(),
                            MachineConfig {
                                id: id.to_string(),
                                channel: channel.to_string(),
                            },
                        );
                    }
                }
                _ => {}
            }
        }

        config
    }

    pub fn get_machine_channel(&self, machine_id: &str) -> Option<String> {
        self.machines.get(machine_id).map(|m| m.channel.clone())
    }
}
