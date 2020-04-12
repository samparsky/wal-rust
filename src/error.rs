use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    Corrupt,
    Closed,
    NotFound,
    OutofOrder,
    OutOfRange,
    InMemoryLog,
    File(std::io::Error)
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Corrupt => write!(f, "log corrupt"),
            Error::Closed => write!(f, "log closed"),
            Error::NotFound => write!(f, "not found"),
            Error::OutofOrder => write!(f, "out of order"),
            Error::OutOfRange => write!(f, "out of range"),
            Error::InMemoryLog => write!(f, "in-memory log not supported"),
            Error::File(e) => write!(f, "file: {}", e),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::File(error)
    }
}
