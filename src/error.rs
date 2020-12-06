use cfg_if::cfg_if;
use r2d2;
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

impl Into<DataOpError> for PlatformError {
    /// attempt to convert platform specific error to DataOpeation error
    fn into(self) -> DataOpError {
        match self {
            #[cfg(feature = "with-postgres")]
            PlatformError::PostgresError(postgres_err) => match postgres_err {
                PostgresError::SqlError(ref pg_err, ref sql) => {
                    if let Some(db_err) = pg_err.as_db() {
                        use crate::TableName;
                        let postgres::error::DbError {
                            severity,
                            code,
                            message,
                            detail,
                            schema,
                            table,
                            column,
                            datatype,
                            constraint,
                            ..
                        } = db_err;

                        DataOpError::ConstraintError {
                            severity: severity.clone(),
                            code: code.code().to_string(),
                            message: message.clone(),
                            detail: detail.clone(),
                            cause_table: if let Some(table) = table {
                                Some(
                                    TableName {
                                        name: table.to_string(),
                                        schema: schema.clone(),
                                        alias: None,
                                    }
                                    .complete_name(),
                                )
                            } else {
                                None
                            },
                            constraint: constraint.clone(),
                            column: column.clone(),
                            datatype: datatype.clone(),
                            sql: sql.to_string(),
                        }
                    } else {
                        DataOpError::GenericError {
                            message: postgres_err.to_string(),
                            sql: None,
                        }
                    }
                }
                _ => DataOpError::GenericError {
                    message: postgres_err.to_string(),
                    sql: None,
                },
            },
            #[cfg(feature = "with-sqlite")]
            PlatformError::SqliteError(e) => DataOpError::GenericError {
                message: e.to_string(),
                sql: None,
            },
            #[cfg(feature = "with-mysql")]
            PlatformError::MysqlError(e) => DataOpError::GenericError {
                message: e.to_string(),
                sql: None,
            },
        }
    }
}

//Note: this is needed coz there is 2 level of variant before we can convert postgres error to
//platform error
#[cfg(feature = "with-postgres")]
impl From<PostgresError> for DbError {
    fn from(e: PostgresError) -> Self {
        DbError::DataOpError(PlatformError::from(e).into())
    }
}

#[cfg(feature = "with-sqlite")]
impl From<rusqlite::Error> for DbError {
    fn from(e: rusqlite::Error) -> Self {
        DbError::DataOpError(PlatformError::SqliteError(SqliteError::from(e)).into())
    }
}

#[cfg(feature = "with-sqlite")]
impl From<SqliteError> for DbError {
    fn from(e: SqliteError) -> Self {
        DbError::DataOpError(PlatformError::SqliteError(e.into()).into())
    }
}

#[cfg(feature = "with-mysql")]
impl From<MysqlError> for DbError {
    fn from(e: MysqlError) -> Self {
        DbError::DataOpError(PlatformError::MysqlError(e.into()).into())
    }
}

#[derive(Debug, Error)]
pub enum DbError {
    #[error("Sql injection attempt error: {0}")]
    SqlInjectionAttempt(String),
    #[error("{0}")]
    DataError(#[from] DataError),
    #[error("{0}")]
    DataOpError(#[from] DataOpError),
    #[error("{0}")]
    ConvertError(#[from] ConvertError),
    #[error("{0}")]
    ConnectError(#[from] ConnectError), //agnostic connection error
    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),
}

#[derive(Debug, Error)]
pub enum DataOpError {
    /// The Data Delete Operation failed due record is still referenced from another table
    #[error("{constraint:?}, {cause_table:?}")]
    ConstraintError {
        severity: String,
        code: String,
        message: String,
        detail: Option<String>,
        cause_table: Option<String>,
        constraint: Option<String>,
        column: Option<String>,
        datatype: Option<String>,
        sql: String,
    },
    #[error("{message}")]
    GenericError {
        message: String,
        sql: Option<String>,
    },
}

#[derive(Debug, Error)]
pub enum ConvertError {
    #[error("Unknown data type")]
    UnknownDataType,
    #[error("Unsupported data type {0}")]
    UnsupportedDataType(String),
}

#[derive(Debug, Error)]
pub enum DataError {
    #[error("Zero record returned")]
    ZeroRecordReturned,
    #[error("More than one record returned")]
    MoreThan1RecordReturned,
    #[error("Table {0} not found")]
    TableNameNotFound(String),
}
