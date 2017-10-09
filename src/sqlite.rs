use r2d2;
use r2d2_sqlite;
use error::DbError;
use error::PlatformError;
use rusqlite;
use database::Database;
use dao::{Value,Rows}; 
use rusqlite::types::{ToSql,ToSqlOutput};
use rusqlite::types::value_ref::ValueRef;

pub fn init_pool(db_url: &str) -> Result<r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>, DbError> {
    let config = r2d2::Config::default();
    let manager = r2d2_sqlite::SqliteConnectionManager::new(db_url);
    r2d2::Pool::new(config, manager)
            .map_err(|e| DbError::PlatformError(
                        PlatformError::SqliteError(
                            SqliteError::PoolInitializationError(e))))
}

pub struct Sqlite(pub r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>);

impl Database for Sqlite{

    fn execute_sql_with_return(&self, sql: &str, param: &[Value]) -> Result<Rows, DbError> {
        let stmt = self.0.prepare(&sql);
        match stmt{
            Ok(stmt) => {
                let sqlite_values = to_sqlite_values(param);
                let sql_types = to_sql_types(&sqlite_values);
                let rows = stmt.query(&sql_types);
                let columns = stmt.column_names();
                let column_names: Vec<String> = columns
                    .iter()
                    .map(|c| c.to_string())
                    .collect();
                 match rows{
                     Ok(rows) => {
                         let mut records = Rows::new(column_names);
                         while let Some(row) = rows.next(){
                             match row{
                                 Ok(row) => {
                                     let mut record: Vec<Value>  = vec![];
                                     for (i, c) in columns.iter().enumerate(){
                                         let value: Result<Value, rusqlite::Error> = row.get_checked(i as i32);
                                         match value {
                                             Ok(value) => record.push(value),
                                             Err(e) => {
                                                 return Err(DbError::PlatformError(
                                                         PlatformError::SqliteError(
                                                             SqliteError::GenericError(e))))
                                             }
                                         }
                                     }
                                     records.push(record);
                                },
                                Err(e) => {
                                     return Err(DbError::PlatformError(
                                             PlatformError::SqliteError(
                                                 SqliteError::GenericError(e))))
                                }
                            }
                         }
                         Ok(records)
                     }
                     Err(e) => {
                         Err(DbError::PlatformError(
                                 PlatformError::SqliteError(
                                     SqliteError::SqlError(e,sql.to_string()))))
                     },
                 }
            },
            Err(e) => {
                Err(DbError::PlatformError(
                        PlatformError::SqliteError(
                            SqliteError::SqlError(e,sql.to_string()))))
            }
        }
    }
}

fn to_sqlite_values(values: &[Value]) -> Vec<SqliteValue> {
    values.iter().map(|v| SqliteValue(v)).collect()
}

fn to_sql_types<'a>(values: &'a Vec<SqliteValue> ) -> Vec<&'a ToSql> {
    values.iter().map(|v| &*v as &ToSql ).collect()
}

/// need to wrap Value in order to be able to implement ToSql trait for it
/// both of which are defined from some other traits
/// otherwise: error[E0117]: only traits defined in the current crate can be implemented for arbitrary types
#[derive(Debug)]
pub struct SqliteValue<'a>(&'a Value);
#[derive(Debug)]
pub struct OwnedSqliteValue(Value);


impl<'a> ToSql for SqliteValue<'a>{

    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput>{
        match *self.0{
            Value::Bool(ref v) => ToSql::to_sql(v),
            _ => panic!("not yet!"),
        }
    }
}

impl FromSql for OwnedSqliteValue{
    fn column_result(value: ValueRef) -> Result<Self, rusqlite::types::from_sql::FromSqlError>{
        panic!("now what?");
    }
}


#[derive(Debug)]
pub enum SqliteError{
    GenericError(rusqlite::Error),
    SqlError(rusqlite::Error, String),
    PoolInitializationError(r2d2::InitializationError),
}
