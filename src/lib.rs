use tracing::Level;
use tracing_subscriber::FmtSubscriber;

pub mod cli;
pub mod collectors;
pub mod config;
pub mod instance;
pub mod util;

pub fn logger_init() {
    // TODO: get debug flag from config or env and set log level.

    // a builder for `FmtSubscriber`.
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than INFO (e.g, info, error, etc.)
        // will be written to stdout.
        .with_max_level(Level::INFO)
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
