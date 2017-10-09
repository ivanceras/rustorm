use r2d2;
use r2d2_sqlite;
use error::DbError;
use error::PlatformError;
use rusqlite;

pub fn init_pool(db_url: &str) -> Result<r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>, DbError> {
    let config = r2d2::Config::default();
    // TODO:: let r2d2_sqlite throw error such as file can't be found
    let manager = r2d2_sqlite::SqliteConnectionManager::new(db_url);
    r2d2::Pool::new(config, manager)
            .map_err(|e| DbError::PlatformError(
                        PlatformError::SqliteError(
                            SqliteError::PoolInitializationError(e))))
}




#[derive(Debug)]
pub enum SqliteError{
    GenericError(rusqlite::Error),
    PoolInitializationError(r2d2::InitializationError),
}
