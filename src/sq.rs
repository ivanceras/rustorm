use r2d2;
use r2d2_sqlite3;
use sqlite3;
use error::DbError;
use error::PlatformError;
use database::Database;
use dao::{Value,Rows}; 
use sqlite3::Type;

pub fn init_pool(db_url: &str) -> Result<r2d2::Pool<r2d2_sqlite3::SqliteConnectionManager>, DbError> {
    let config = r2d2::Config::default();
    let manager = r2d2_sqlite3::SqliteConnectionManager::file(db_url);
    r2d2::Pool::new(config, manager)
            .map_err(|e| DbError::PlatformError(
                        PlatformError::SqliteError(
                            SqliteError::PoolInitializationError(e))))
}

pub struct Sqlite(pub r2d2::PooledConnection<r2d2_sqlite3::SqliteConnectionManager>);

impl Database for Sqlite{

    fn execute_sql_with_return(&self, sql: &str, param: &[Value]) -> Result<Rows, DbError> {
        let mut stmt = self.0.prepare(&sql);
        match stmt{
            Ok(mut stmt) => {
                let column_names = stmt.column_names()
                    .map_err(|e| 
                            DbError::PlatformError(
                                PlatformError::SqliteError(
                                    SqliteError::GenericError(e))))?;
                let column_names: Vec<String> = column_names
                    .iter()
                    .map(|c| c.to_string())
                    .collect();
                 let mut records = Rows::new(column_names);
                 while let Ok(row) = stmt.next(){
                     let mut record: Vec<Value>  = vec![];
                     for i in 0..stmt.columns(){
                         macro_rules! match_type {
                             ($ty: ty, $variant: ident) => {
                                     {
                                      let raw: Result<$ty,sqlite3::Error> = stmt.read(i);
                                      match raw{
                                          Ok(raw) => Ok(Value::$variant(raw)),
                                          Err(e) => Err(DbError::PlatformError(
                                                     PlatformError::SqliteError(
                                                         SqliteError::GenericError(e)))),
                                     }
                                 }
                             }
                         }
                         let ty = stmt.kind(i);
                         let value:Result<Value,DbError> = 
                             match ty{
                                Type::Binary => match_type!(Vec<u8>, Blob),
                                Type::Float => match_type!(f64, Double),
                                Type::Integer => match_type!(i64, Bigint),
                                Type::String => match_type!(String, Text),
                                Type::Null => Ok(Value::Nil),
                            };
                         record.push(value?);
                     }
                     records.push(record);
                 }
                 Ok(records)
            },
            Err(e) => {
                Err(DbError::PlatformError(
                        PlatformError::SqliteError(
                            SqliteError::SqlError(e,sql.to_string()))))
            }
        }
    }
}




#[derive(Debug)]
pub enum SqliteError{
    GenericError(sqlite3::Error),
    SqlError(sqlite3::Error, String),
    PoolInitializationError(r2d2::InitializationError),
}


#[cfg(test)]
mod test{

}
