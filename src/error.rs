use std::fmt;

#[derive(Debug, Clone)]
pub enum Error {
    Corrupt,
    Closed,
    NotFound,
    OutofOrder,
    OutOfRange,
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
        }
    }
}