use actix_web::rt::task::JoinError;
pub(crate) use derive_more::derive::{Display, Error};

#[derive(Debug, Display, Error)]
#[display("my error: {err}")]
pub struct CustomError {
    err: anyhow::Error,
}

impl actix_web::error::ResponseError for CustomError {}

impl From<anyhow::Error> for CustomError {
    fn from(err: anyhow::Error) -> CustomError {
        CustomError { err }
    }
}

impl From<JoinError> for CustomError {
    fn from(err: JoinError) -> CustomError {
        CustomError { err: err.into() }
    }
}
