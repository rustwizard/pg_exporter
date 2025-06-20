use actix_web::rt::task::JoinError;
pub(crate) use derive_more::derive::{Display, Error};

#[derive(Debug, Display, Error)]
#[display("my error: {err}")]
pub struct MyError {
    err: anyhow::Error,
}

impl actix_web::error::ResponseError for MyError {}

impl From<anyhow::Error> for MyError {
    fn from(err: anyhow::Error) -> MyError {
        MyError { err }
    }
}

impl From<JoinError> for MyError {
    fn from(err: JoinError) -> MyError {
        MyError { err: err.into() }
    }
}
