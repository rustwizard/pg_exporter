use tracing_subscriber::EnvFilter;
use tracing_subscriber::FmtSubscriber;

pub mod app;
pub mod cli;
pub mod collectors;
pub mod config;
pub mod instance;
pub mod util;

pub fn logger_init() {
    let subscriber = FmtSubscriber::builder()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
