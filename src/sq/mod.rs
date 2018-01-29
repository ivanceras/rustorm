use r2d2;
use r2d2_sqlite3;
use sqlite3;
use error::DbError;
use error::PlatformError;
use database::Database;
use dao::{Value,Rows}; 
use sqlite3::Type;
use table::Table;
use entity::EntityManager;
use dao::TableName;
use table::SchemaContent;
use nom_sql;
use r2d2::ManageConnection;

pub fn init_pool(db_url: &str) -> Result<r2d2::Pool<r2d2_sqlite3::SqliteConnectionManager>, SqliteError> {
    println!("initializing pool: {}", db_url);
    let manager = r2d2_sqlite3::SqliteConnectionManager::file(db_url);
    let pool = r2d2::Pool::new(manager)?;
    Ok(pool)
}

pub fn test_connection(db_url: &str) -> Result<(), SqliteError> {
    let manager = r2d2_sqlite3::SqliteConnectionManager::file(db_url);
    let mut conn = manager.connect()?;
    manager.is_valid(&mut conn)?;
    Ok(())
}

pub struct SqliteDB(pub r2d2::PooledConnection<r2d2_sqlite3::SqliteConnectionManager>);

fn to_sq_value(val: &Value) -> sqlite3::Value {
    use num_traits::ToPrimitive;
    match *val{
        Value::Text(ref v) => sqlite3::Value::String(v.to_owned()),
        Value::Bool(v) => {
            sqlite3::Value::Integer(if v{1}else{0})
        }
        Value::Tinyint(v) => {
            sqlite3::Value::Integer(v as i64)
        }
        Value::Smallint(v) => {
            sqlite3::Value::Integer(v as i64)
        }
        Value::Int(v) => {
            sqlite3::Value::Integer(v as i64)
        }
        Value::Bigint(v) => {
            sqlite3::Value::Integer(v as i64)
        }

        Value::Float(v) => {
            sqlite3::Value::Float(v as f64)
        }
        Value::Double(v) => {
            sqlite3::Value::Float(v as f64)
        }
        Value::BigDecimal(ref v) => {
            match v.to_f64(){
                Some(v) => sqlite3::Value::Float(v as f64),
                None => panic!("unable to convert bigdecimal"), 
            }
        }
        Value::Blob(ref v) => sqlite3::Value::Binary(v.clone()),
        Value::ImageUri(ref v) => sqlite3::Value::String(v.clone()),
        Value::Char(v) => sqlite3::Value::String(format!("{}", v)),
        Value::Json(ref v) => sqlite3::Value::String(v.clone()),
        Value::Uuid(ref v) => sqlite3::Value::String(v.to_string()),
        Value::Date(ref v) =>  sqlite3::Value::String(v.to_string()),
        Value::Nil => sqlite3::Value::Null,
        _ => panic!("not yet handled: {:?}", val),
    }
}

fn to_sq_values(params: &[Value]) -> Vec<sqlite3::Value> {
    let mut sql_values = Vec::with_capacity(params.len());
    for param in params{
        let sq_val = to_sq_value(param);
        sql_values.push(sq_val);
    }
    sql_values
}

impl Database for SqliteDB{

    fn execute_sql_with_return(&self, sql: &str, params: &[Value]) -> Result<Rows, DbError> {
        println!("executing sql: {}", sql);
        println!("params: {:?}", params);
        let stmt = self.0.prepare(&sql);
        match stmt{
            Ok(mut stmt) => {
                let sq_values = to_sq_values(params);
                println!("sq_values: {:#?}", sq_values);
                for (i,sq_val) in sq_values.iter().enumerate(){
                    stmt.bind(i + 1, sq_val)?;
                }
                let column_names = stmt.column_names()
                    .map_err(|e| 
                            DbError::PlatformError(
                                PlatformError::SqliteError(
                                    SqliteError::GenericError(e))))?;
                println!("column names: {:?}", column_names);
                let column_names: Vec<String> = column_names
                    .iter()
                    .map(|c| c.to_string())
                    .collect();
                 let mut records = Rows::new(column_names);
                 if let Ok(sqlite3::State::Row) = stmt.next(){
                     println!("HERE <<---");
                     let mut record: Vec<Value>  = vec![];
                     for i in 0..stmt.columns(){
                         macro_rules! match_type {
                             ($ty: ty, $variant: ident) => {
                                     {
                                      let raw: Result<$ty,sqlite3::Error> = stmt.read(i);
                                      println!("raw: {:?}", raw);
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
                 println!("records: {:#?}", records);
                 Ok(records)
            },
            Err(e) => {
                Err(DbError::PlatformError(
                        PlatformError::SqliteError(
                            SqliteError::SqlError(e,sql.to_string()))))
            }
        }
    }

    #[allow(unused_variables)]
    fn get_table(&self, em: &EntityManager, table_name: &TableName) -> Result<Table, DbError> {
        panic!("sqlite under construction")
    }

    #[allow(unused_variables)]
    fn get_all_tables(&self, em: &EntityManager) -> Result<Vec<Table>, DbError> {
        panic!("sqlite under construction")
    }
    #[allow(unused_variables)]
    fn get_grouped_tables(&self, em: &EntityManager) -> Result<Vec<SchemaContent>, DbError> {
        panic!("not yet!");
    }
}


fn extract_table_sql(em: &EntityManager, table_name: &String){
    use dao;
    use dao::FromDao;
    #[derive(Debug, FromDao)]
    struct TableSql{
        name: String,
        sql: String,
    }
    let sql = "SELECT name, sql FROM sqlite_master WHERE type = 'table' and name = ?";
    let result:Result<TableSql,DbError> = em.execute_sql_with_one_return(&sql, &[&table_name]);
    println!("result: {:#?}", result);
    match result{
        Ok(table_sql) => {
            println!("parsing: {}", table_sql.sql);
            //let corrected_sql = table_sql.sql.replace("numeric", "bigint(20)");
            let corrected_sql = table_sql.sql;
            let query = nom_sql::parser::parse_query(&corrected_sql);
            println!("query: {:#?}", query);
        }
        Err(e) => {
            panic!("error ")
        }
    }
}




#[derive(Debug)]
pub enum SqliteError{
    GenericError(sqlite3::Error),
    SqlError(sqlite3::Error, String),
    PoolInitializationError(r2d2::Error),
}

impl From<r2d2::Error> for SqliteError{
    fn from(e: r2d2::Error) -> Self {
        SqliteError::PoolInitializationError(e)
    }
}

impl From<sqlite3::Error> for SqliteError {
    fn from(e: sqlite3::Error) -> Self {
        SqliteError::GenericError(e)
    }
}


#[cfg(test)]
mod test{
    use super::*;
    use pool::Pool;
    use pool;

    #[test]
    fn test_conn(){
        let db_url = "sqlite://sakila.db";
        let result = pool::test_connection(db_url);
        println!("result: {:?}", result);
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_extract_sql(){
        let db_url = "sqlite://sakila.db";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let film = "actor".to_string();
        extract_table_sql(&em, &film);
        panic!();
    }
}
