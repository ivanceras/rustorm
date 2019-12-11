use std::error::Error;
use std::fmt;
use std::fmt::Debug;

#[derive(Debug)]
pub enum ConvertError {
    NotSupported(String, String),
}

impl Error for ConvertError {
    fn description(&self) -> &str {
        "Conversion is not supported"
    }
}

impl fmt::Display for ConvertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

#[derive(Debug)]
pub enum DaoError {
    ConvertError(ConvertError),
    NoSuchValueError(String),
}
