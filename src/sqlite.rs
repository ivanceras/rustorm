
pub fn init_pool(db_url: &str) -> Result<r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>, DbError> {
    let config = r2d2::Config::default();
    let manager = r2d2_sqlite::SqliteConnectionManager::new(db_url);
    let pool = r2d2::Pool::new(config, manager).unwrap();
}
