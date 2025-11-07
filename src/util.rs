/// Get PgExporter's version string.
pub fn version() -> String {
    format!("v{} [main@{}]", env!("CARGO_PKG_VERSION"), env!("GIT_HASH"))
}
