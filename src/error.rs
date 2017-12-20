use url;
use std::error::Error;
use std::fmt;
use r2d2;
cfg_if! {if #[cfg(feature = "with-postgres")]{
    use pg::PostgresError;
}}

cfg_if! {if #[cfg(feature = "with-sqlite")]{
    use sq::SqliteError;
}}

#[derive(Debug)]
pub enum ConnectError {
    NoSuchPoolConnection,
    ParseError(ParseError),
    UnsupportedDb(String),
    R2d2Error(r2d2::Error),
}

/// TODO: use error_chain i guess?
impl Error for ConnectError {
    fn description(&self) -> &str {
        "short desc"
    }
    fn cause(&self) -> Option<&Error> {
        None
    }
}

impl fmt::Display for ConnectError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}

#[derive(Debug)]
pub enum ParseError {
    DbUrlParseError(url::ParseError),
}

#[derive(Debug)]
pub enum PlatformError {
    #[cfg(feature = "with-postgres")] PostgresError(PostgresError),
    #[cfg(feature = "with-sqlite")] SqliteError(SqliteError),
}

#[derive(Debug)]
pub enum DbError {
    DataError(DataError),
    PlatformError(PlatformError),
    ConvertError(ConvertError),
    ConnectError(ConnectError), //agnostic connection error
}

#[derive(Debug)]
pub enum ConvertError {
    UnknownDataType,
    UnsupportedDataType(String),
}

#[derive(Debug)]
pub enum DataError {
    ZeroRecordReturned,
    MoreThan1RecordReturned,
}
