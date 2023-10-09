use stac::Value;
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("invalid STAC")]
    InvalidValue(Value),

    #[error(transparent)]
    StacAsync(#[from] stac_async::Error),

    #[error(transparent)]
    StacValidate(#[from] stac_validate::Error),

    #[error(transparent)]
    TokioJoinError(#[from] tokio::task::JoinError),
}

impl Error {
    pub fn return_code(&self) -> i32 {
        // TODO make these codes more meaningful
        1
    }
}
