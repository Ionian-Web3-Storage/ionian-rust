use std::error::Error as ErrorTrait;
use std::fmt::{Debug, Display, Formatter};
use std::io::Error as IoError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(IoError),
    /// A partial chunk batch is written.
    InvalidBatchBoundary,
    Custom(String),
}

impl From<IoError> for Error {
    fn from(e: IoError) -> Self {
        Error::Io(e)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StorageError: {:?}", self)
    }
}

impl ErrorTrait for Error {}
