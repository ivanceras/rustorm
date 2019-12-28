use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConvertError {
    #[error("Conversion not supported {0} to {1}")]
    NotSupported(String, String),
}


#[derive(Error, Debug)]
pub enum DaoError {
    #[error("ConvertError {0}")]
    ConvertError(ConvertError),
    #[error("No such value {0}")]
    NoSuchValueError(String),
}
