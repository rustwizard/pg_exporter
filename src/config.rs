use config_file::FromConfigFile;
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    host: String,
}
