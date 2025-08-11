use actix_web::rt::task::JoinError;
use anyhow::anyhow;

use thiserror::Error;

#[derive(Error, Debug)]
#[error(transparent)]
pub struct MetricsError {
    err: anyhow::Error,
}

impl actix_web::error::ResponseError for MetricsError {}

impl From<anyhow::Error> for MetricsError {
    fn from(err: anyhow::Error) -> MetricsError {
        MetricsError { err }
    }
}

impl From<JoinError> for MetricsError {
    fn from(err: JoinError) -> MetricsError {
        MetricsError { err: anyhow!("internal error: {:?}", err) }
    }
}

impl From<prometheus::Error> for MetricsError {
    fn from(err: prometheus::Error) -> MetricsError {
        MetricsError { err: anyhow!("internal error: {:?}", err) }
    }
}


