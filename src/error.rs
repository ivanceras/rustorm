use url;
use std::error::Error;
use std::fmt;
use r2d2;
cfg_if! {if #[cfg(feature = "with-postgres")]{
    use pg::PostgresError;
}}

cfg_if! {if #[cfg(feature = "with-sqlite")]{
    use sq::SqliteError;
    use sqlite3;
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
    #[cfg(feature = "with-postgres")]
    PostgresError(PostgresError),
    #[cfg(feature = "with-sqlite")]
    SqliteError(SqliteError),
}

#[cfg(feature = "with-postgres")]
impl From<PostgresError> for PlatformError {
    fn from(e: PostgresError) -> Self {
        PlatformError::PostgresError(e)
    }
}

#[cfg(feature = "with-postgres")]
impl From<PostgresError> for DbError {
    fn from(e: PostgresError) -> Self {
        DbError::PlatformError(PlatformError::from(e))
    }
}

#[cfg(feature = "with-sqlite")]
impl From<sqlite3::Error> for DbError {
    fn from(e: sqlite3::Error) -> Self {
        DbError::PlatformError(PlatformError::SqliteError(SqliteError::from(e)))
    }
}

#[cfg(feature = "with-sqlite")]
impl From<SqliteError> for PlatformError {
    fn from(e: SqliteError) -> Self {
        PlatformError::SqliteError(e)
    }
}

#[cfg(feature = "with-sqlite")]
impl From<SqliteError> for DbError {
    fn from(e: SqliteError) -> Self {
        DbError::PlatformError(PlatformError::from(e))
    }
}

#[derive(Debug)]
pub enum DbError {
    SqlInjectionAttempt(String),
    DataError(DataError),
    PlatformError(PlatformError),
    ConvertError(ConvertError),
    ConnectError(ConnectError), //agnostic connection error
}

impl From<PlatformError> for DbError {
    fn from(e: PlatformError) -> Self {
        DbError::PlatformError(e)
    }
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
