use cfg_if::cfg_if;
use r2d2;
use serde::Serialize;
use serde::Serializer;
use thiserror::Error;
use url;

cfg_if! {if #[cfg(feature = "with-postgres")]{
    use crate::pg::PostgresError;
}}

cfg_if! {if #[cfg(feature = "with-sqlite")]{
    use crate::sqlite::SqliteError;
    use rusqlite;
}}

cfg_if! {if #[cfg(feature = "with-mysql")]{
    use crate::my::MysqlError;
}}

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error("No such pool connection")]
    NoSuchPoolConnection,
    #[error("{0}")]
    ParseError(#[from] ParseError),
    #[error("Database not supported: {0}")]
    UnsupportedDb(String),
    #[error("{0}")]
    R2d2Error(#[from] r2d2::Error),
}

impl Serialize for ConnectError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ConnectError::NoSuchPoolConnection => {
                serializer.serialize_newtype_struct("NoSuchPoolConnection", &())
            }
            ConnectError::ParseError(e) => {
                serializer.serialize_newtype_struct("ParseError", &e.to_string())
            }
            ConnectError::UnsupportedDb(e) => {
                serializer.serialize_newtype_struct("UnsupportedDb", e)
            }
            ConnectError::R2d2Error(e) => {
                serializer.serialize_newtype_struct("R2d2Error", &e.to_string())
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Database url parse error: {0}")]
    DbUrlParseError(#[from] url::ParseError),
}

#[derive(Debug, Error)]
#[error("{0}")]
pub enum PlatformError {
    #[cfg(feature = "with-postgres")]
    #[error("{0}")]
    PostgresError(#[from] PostgresError),
    #[cfg(feature = "with-sqlite")]
    #[error("{0}")]
    SqliteError(#[from] SqliteError),
    #[cfg(feature = "with-mysql")]
    #[error("{0}")]
    MysqlError(#[from] MysqlError),
}

impl Serialize for PlatformError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            #[cfg(feature = "with-postgres")]
            PlatformError::PostgresError(e) => {
                serializer.serialize_newtype_variant("PlatformError", 0, "PostgresError", e)
            }
            #[cfg(feature = "with-sqlite")]
            PlatformError::SqliteError(e) => {
                serializer.serialize_newtype_variant("PlatformError", 1, "SqliteError", e)
            }
            #[cfg(feature = "with-mysql")]
            PlatformError::MysqlError(e) => {
                serializer.serialize_newtype_variant("PlatformError", 2, "MysqlError", e)
            }
        }
    }
}

//Note: this is needed coz there is 2 level of variant before we can convert postgres error to
//platform error
#[cfg(feature = "with-postgres")]
impl From<PostgresError> for DbError {
    fn from(e: PostgresError) -> Self {
        DbError::PlatformError(PlatformError::from(e))
    }
}

#[cfg(feature = "with-sqlite")]
impl From<rusqlite::Error> for DbError {
    fn from(e: rusqlite::Error) -> Self {
        DbError::PlatformError(PlatformError::SqliteError(SqliteError::from(e)))
    }
}

#[cfg(feature = "with-sqlite")]
impl From<SqliteError> for DbError {
    fn from(e: SqliteError) -> Self {
        DbError::PlatformError(PlatformError::from(e))
    }
}

#[cfg(feature = "with-mysql")]
impl From<MysqlError> for DbError {
    fn from(e: MysqlError) -> Self {
        DbError::PlatformError(PlatformError::from(e))
    }
}

#[derive(Debug, Error, Serialize)]
pub enum DbError {
    #[error("Sql injection attempt error: {0}")]
    SqlInjectionAttempt(String),
    #[error("{0}")]
    DataError(#[from] DataError),
    #[error("{0}")]
    PlatformError(#[from] PlatformError),
    #[error("{0}")]
    ConvertError(#[from] ConvertError),
    #[error("{0}")]
    ConnectError(#[from] ConnectError), //agnostic connection error
    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),
}

#[derive(Debug, Error, Serialize)]
pub enum ConvertError {
    #[error("Unknown data type")]
    UnknownDataType,
    #[error("Unsupported data type {0}")]
    UnsupportedDataType(String),
}

#[derive(Debug, Error, Serialize)]
pub enum DataError {
    #[error("Zero record returned")]
    ZeroRecordReturned,
    #[error("More than one record returned")]
    MoreThan1RecordReturned,
}
