use crate::utils::vini::ConfigIni;
use r2d2::Pool;
use r2d2_firebird::FirebirdConnectionManager;
use rsfbclient::{charset::WIN_1252, PureRustConnectionBuilder};

use std::sync::Arc;
use std::time::Duration;

pub fn initiate_firebird_connection(
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
