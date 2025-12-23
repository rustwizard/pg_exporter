use tracing::Level;
use tracing_subscriber::FmtSubscriber;

pub fn setup_tracing() {
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than INFO (e.g, info, error, etc.)
        // will be written to stdout.
        .with_max_level(Level::INFO)
        // completes the builder.
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);
}
