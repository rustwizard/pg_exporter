use tracing_subscriber::EnvFilter;
use tracing_subscriber::FmtSubscriber;

pub mod app;
pub mod cli;
pub mod collectors;
pub mod config;
pub mod instance;
pub mod util;

pub fn logger_init(level: Option<&str>) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        level
            .and_then(|l| EnvFilter::try_new(l).ok())
            .unwrap_or_else(|| EnvFilter::new("info"))
    });

    let subscriber = FmtSubscriber::builder().with_env_filter(filter).finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
