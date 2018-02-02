use r2d2;
use r2d2_postgres;
use r2d2_postgres::TlsMode;
use database::Database;
use dao::{Value};
use error::DbError;
use dao::Rows;
use postgres;
use postgres::types::{self,ToSql,FromSql,Type,IsNull};
use error::PlatformError;
use std::error::Error;
use std::fmt;
use bigdecimal::BigDecimal;
use dao::TableName;
use table::Table;
use entity::EntityManager;
use dao::value::Array;
use table::SchemaContent;
use std::string::FromUtf8Error;
use postgres_shared::types::Kind::Enum;
use postgres_shared::types::Kind;
use self::numeric::PgNumeric;
use r2d2::ManageConnection;
use tree_magic;
use base64;


mod table_info;
mod column_info;
mod numeric;

pub fn init_pool(db_url: &str) -> Result<r2d2::Pool<r2d2_postgres::PostgresConnectionManager>, PostgresError>{
    test_connection(db_url)?;
    let manager = r2d2_postgres::PostgresConnectionManager::new(db_url, TlsMode::None)?;
    let pool = r2d2::Pool::new(manager)?;
    Ok(pool)
}

pub fn test_connection(db_url: &str) -> Result<(), PostgresError> {
    let manager = r2d2_postgres::PostgresConnectionManager::new(db_url, TlsMode::None)?;
    let mut conn = manager.connect()?;
    manager.is_valid(&mut conn)?;
    Ok(())
}

pub struct PostgresDB(pub r2d2::PooledConnection<r2d2_postgres::PostgresConnectionManager>);

impl Database for PostgresDB{

    
    fn execute_sql_with_return(&self, sql: &str, param: &[Value]) -> Result<Rows, DbError> {
        let stmt = self.0.prepare(&sql);
        match stmt{
            Ok(stmt) => {
                let pg_values = to_pg_values(param);
                let sql_types = to_sql_types(&pg_values);
                let rows = stmt.query(&sql_types);
                match rows {
                    Ok(rows) => {
                        let columns = rows.columns();
                        let column_names:Vec<String> = columns
                            .iter()
                            .map(|c| c.name().to_string() )
                            .collect();
                        let mut records = Rows::new(column_names);
                        for r in rows.iter(){
                            let mut record:Vec<Value> = vec![];
                            for (i,column) in columns.iter().enumerate(){
                                let value: Option<Result<OwnedPgValue, postgres::Error>> = r.get_opt(i);
                                match value{
                                    Some(value) => {
                                        match value{
                                            Ok(value) =>  record.push(value.0),
                                            Err(e) => {
                                                //println!("Row {:?}", r);
                                                println!("column {:?} index: {}", column, i);
                                                let msg = format!("Error converting column {:?} at index {}", column, i);
                                                return Err(DbError::PlatformError(
                                                        PlatformError::PostgresError(PostgresError::GenericError(msg, e))))
                                            }
                                        }
                                    },
                                    None => {
                                        record.push(Value::Nil);// Note: this is important to not mess the spacing of records
                                    }
                                }
                            }
                            records.push(record);
                        }
                        Ok(records)
                    },
                    Err(e) => Err(DbError::PlatformError(
                            PlatformError::PostgresError(
                                PostgresError::SqlError(e, sql.to_string())))),
                }
            },
            Err(e) => Err(DbError::PlatformError(
                    PlatformError::PostgresError(
                        PostgresError::SqlError(e, sql.to_string()))))
        }
    }

    fn get_table(&self, em: &EntityManager, table_name: &TableName) -> Result<Table, DbError> {
        table_info::get_table(em, table_name)
    }

    fn get_all_tables(&self, em: &EntityManager) -> Result<Vec<Table>, DbError> {
        table_info::get_all_tables(em)
    }

    fn get_grouped_tables(&self, em: &EntityManager) -> Result<Vec<SchemaContent>, DbError> {
        table_info::get_organized_tables(em)
    }

}



fn to_pg_values(values: &[Value]) -> Vec<PgValue> {
    values.iter().map(|v| PgValue(v)).collect()
}

fn to_sql_types<'a>(values: &'a Vec<PgValue> ) -> Vec<&'a ToSql> {
    let mut sql_types = vec![];
    for v in values.iter(){
        sql_types.push(&*v as &ToSql);
    }
    sql_types
}




/// need to wrap Value in order to be able to implement ToSql trait for it
/// both of which are defined from some other traits
/// otherwise: error[E0117]: only traits defined in the current crate can be implemented for arbitrary types
/// For inserting, implement only ToSql
#[derive(Debug)]
pub struct PgValue<'a>(&'a Value);

/// need to wrap Value in order to be able to implement ToSql trait for it
/// both of which are defined from some other traits
/// otherwise: error[E0117]: only traits defined in the current crate can be implemented for arbitrary types
/// For retrieval, implement only FromSql
#[derive(Debug)]
pub struct OwnedPgValue(Value);

impl<'a> ToSql for PgValue<'a>{
    fn to_sql( &self, ty: &Type, out: &mut Vec<u8>) 
        -> Result<IsNull, Box<Error + 'static + Sync + Send>>{
        match *self.0{
            Value::Bool(ref v) => v.to_sql(ty, out),
            Value::Tinyint(ref v) => v.to_sql(ty, out),
            Value::Smallint(ref v) => v.to_sql(ty, out),
            Value::Int(ref v) => v.to_sql(ty, out),
            Value::Bigint(ref v) => v.to_sql(ty, out),
            Value::Float(ref v) => v.to_sql(ty, out),
            Value::Double(ref v) => v.to_sql(ty, out),
            Value::Blob(ref v) => v.to_sql(ty, out),
            Value::ImageUri(ref _v) => panic!("ImageUri is only used for reading data from DB, not inserting into DB"),
            Value::Char(ref v) => v.to_string().to_sql(ty, out),
            Value::Text(ref v) => v.to_sql(ty, out),
            Value::Uuid(ref v) => v.to_sql(ty, out),
            Value::Date(ref v) => v.to_sql(ty, out),
            Value::Timestamp(ref v) => v.to_sql(ty, out),
            Value::DateTime(ref v) => v.to_sql(ty, out),
            Value::Time(ref v) => v.to_sql(ty, out),
            Value::BigDecimal(ref v) => {
                let numeric: PgNumeric = v.into();
                numeric.to_sql(ty, out)
            }
            Value::Json(ref v) => v.to_sql(ty, out),
            Value::Array(ref v) => 
                match *v{
                    Array::Text(ref av) => av.to_sql(ty, out),
                    Array::Int(ref av) => av.to_sql(ty, out),
                    Array::Float(ref av) => av.to_sql(ty, out),
                }
            Value::Nil => Ok(IsNull::Yes),
        }
    }

    fn accepts(_ty: &Type) -> bool{
        true 
    }

    to_sql_checked!();
}

impl FromSql for OwnedPgValue{
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<Error + Sync + Send>>{
        macro_rules! match_type {
            ($variant: ident ) => {
                FromSql::from_sql(ty, raw).map(|v|OwnedPgValue(Value::$variant(v)))
            }
        }
        let kind = ty.kind();
        println!("kind: {:?}", kind);
        match *kind{
            Enum(_) => match_type!(Text),
            Kind::Array(ref array_type) => {
                let array_type_kind = array_type.kind();
                match *array_type_kind{
                    Enum(_) => {
                        FromSql::from_sql(ty, raw)
                            .map(|v|OwnedPgValue(Value::Array(Array::Text(v))))
                    }
                    _ => {
                        match *ty{
                            types::TEXT_ARRAY 
                                | types::NAME_ARRAY 
                                | types::VARCHAR_ARRAY => {
                                    FromSql::from_sql(ty, raw)
                                        .map(|v|OwnedPgValue(Value::Array(Array::Text(v))))
                                }
                            types::INT4_ARRAY => {
                                    FromSql::from_sql(ty, raw)
                                        .map(|v|OwnedPgValue(Value::Array(Array::Int(v))))
                            }
                            types::FLOAT4_ARRAY => {
                                    FromSql::from_sql(ty, raw)
                                        .map(|v|OwnedPgValue(Value::Array(Array::Float(v))))
                            }
                            _ => panic!("Array type {:?} is not yet covered", array_type),
                        }
                    }
                }
            },
            Kind::Simple => {
                match *ty {
                    types::BOOL => match_type!(Bool), 
                    types::INT2  => match_type!(Smallint),
                    types::INT4  => match_type!(Int),
                    types::INT8  => match_type!(Bigint),
                    types::FLOAT4 => match_type!(Float),
                    types::FLOAT8 => match_type!(Double),
                    types::TEXT | types::VARCHAR | types::NAME | types::UNKNOWN => match_type!(Text),
                    types::TS_VECTOR => {
                        let text = String::from_utf8(raw.to_owned());
                        match text{
                            Ok(text) => Ok(OwnedPgValue(Value::Text(text))),
                            Err(e) => Err(Box::new(PostgresError::FromUtf8Error(e))),
                        }
                    }
                    types::BPCHAR => {
                        let v: Result<String,_> = FromSql::from_sql(&types::TEXT, raw);
                        match v{
                            Ok(v) => {
                                if v.chars().count() == 1 {
                                    Ok( OwnedPgValue(Value::Char(v.chars().next().unwrap())))
                                }else {
                                    FromSql::from_sql(ty, raw).map(|v|OwnedPgValue(Value::Text(v)))
                                }
                            },
                            Err(e) => Err(e)
                        }
                    }
                    types::UUID => match_type!(Uuid),
                    types::DATE => match_type!(Date),
                    types::TIMESTAMPTZ | types::TIMESTAMP => match_type!(Timestamp),
                    types::TIME | types::TIMETZ => match_type!(Time),
                    types::BYTEA => {
                        let mime_type = tree_magic::from_u8(raw);
                        println!("mime_type: {}", mime_type);
                        let bytes:Vec<u8> = FromSql::from_sql(ty, raw).unwrap();
                        //assert_eq!(raw, &*bytes);
                        let base64 = base64::encode_config(&bytes, base64::MIME);
                        match &*mime_type {
                            "image/jpeg"|
                            "image/png" => {
                                Ok(OwnedPgValue(Value::ImageUri(format!("data:{};base64,{}",mime_type,base64))))
                            }
                            _ => {
                                match_type!(Blob)
                            }
                        }
                    }
                    types::NUMERIC => {
                        let numeric: PgNumeric = FromSql::from_sql(ty, raw)?;
                        let bigdecimal = BigDecimal::from(numeric);
                        Ok(OwnedPgValue(Value::BigDecimal(bigdecimal)))
                    }
                    types::JSON => {
                        let text = String::from_utf8(raw.to_owned());
                        match text{
                            Ok(text) => Ok(OwnedPgValue(Value::Json(text))),
                            Err(e) => Err(Box::new(PostgresError::FromUtf8Error(e))),
                        }
                    }
                    types::INET => {
                        println!("inet raw:{:?}", raw);
                        match_type!(Text)
                    }
                    _ => panic!("unable to convert from {:?}", ty), 
                }
            }
            _ => panic!("not yet handling this kind: {:?}", kind),
        }


    }
    fn accepts(_ty: &Type) -> bool{
        true
    }

    fn from_sql_null(_ty: &Type) -> Result<Self, Box<Error + Sync + Send>> { 
        Ok(OwnedPgValue(Value::Nil))
    }
    fn from_sql_nullable(
        ty: &Type, 
        raw: Option<&[u8]>
    ) -> Result<Self, Box<Error + Sync + Send>> { 
        match raw{
            Some(raw) => Self::from_sql(ty, raw),
            None => Self::from_sql_null(ty), 
        }

    }
}


#[cfg(test)]
mod test{

    use super::*;
    use pool::{Pool, PooledConn};
    use postgres::Connection;
    use std::ops::Deref;
    use dao::Value;
    use dao::Rows;

    #[test]
    fn connect_test_query(){
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let conn = pool.connect(db_url);
        assert!(conn.is_ok());
        let conn: PooledConn = conn.unwrap();
        match conn{
            PooledConn::PooledPg(ref pooled_pg) => {
                let rows = pooled_pg.query("select 42, 'life'", &[]).unwrap();
                for row in rows.iter(){
                    let n: i32 = row.get(0);
                    let l: String = row.get(1);
                    assert_eq!(n, 42);
                    assert_eq!(l, "life");
                }
            }
        #[cfg(any(feature = "with-sqlite"))]
            _ => unreachable!()
        }
    }
    #[test]
    fn connect_test_query_explicit_deref(){
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let conn = pool.connect(db_url);
        assert!(conn.is_ok());
        let conn: PooledConn = conn.unwrap();
        match conn{
            PooledConn::PooledPg(ref pooled_pg) => {
                let c: &Connection = pooled_pg.deref(); //explicit deref here
                let rows = c.query("select 42, 'life'", &[]).unwrap();
                for row in rows.iter(){
                    let n: i32 = row.get(0);
                    let l: String = row.get(1);
                    assert_eq!(n, 42);
                    assert_eq!(l, "life");
                }
            }
        #[cfg(any(feature = "with-sqlite"))]
            _ => unreachable!()
        }
    }
    #[test]
    fn test_unknown_type(){
        let mut pool = Pool::new();
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let db  = pool.db(db_url).unwrap();
        let values:Vec<Value> = vec![
            "hi".into(),
            true.into(),
            42.into(),
            1.0.into(),
        ];
        let rows:Result<Rows, DbError> = (&db).execute_sql_with_return("select 'Hello', $1::TEXT, $2::BOOL, $3::INT, $4::FLOAT", &values);
        println!("rows: {:#?}", rows);
        assert!(rows.is_ok());
    }
    #[test]
    // only text can be inferred to UNKNOWN types
    fn test_unknown_type_i32_f32(){
        let mut pool = Pool::new();
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let db  = pool.db(db_url).unwrap();
        let values:Vec<Value> = vec![
            42.into(),
            1.0.into(),
        ];
        let rows:Result<Rows, DbError> = (&db).execute_sql_with_return("select $1, $2", &values);
        println!("rows: {:#?}", rows);
        assert!(!rows.is_ok());
    }

    #[test]
    fn using_values(){
        let mut pool = Pool::new();
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let db  = pool.db(db_url).unwrap();
        let values:Vec<Value> = vec![
            "hi".into(),
            true.into(),
            42.into(),
            1.0.into(),
        ];
        let rows:Result<Rows, DbError> = (&db).execute_sql_with_return("select 'Hello'::TEXT, $1::TEXT, $2::BOOL, $3::INT, $4::FLOAT", &values);
        println!("columns: {:#?}", rows);
        assert!(rows.is_ok());
        if let Ok(rows) = rows {
            for row in rows.iter(){
                println!("row {:?}", row);
                let v4:Result<f64, _> = row.get("float8");
                assert_eq!(v4.unwrap(), 1.0f64);

                let v3:Result<i32, _> = row.get("int4");
                assert_eq!(v3.unwrap(), 42i32);

                let hi: Result<String, _> = row.get("text");
                assert_eq!(hi.unwrap(), "hi");
                
                let b: Result<bool, _> = row.get("bool");
                assert_eq!(b.unwrap(), true);
            }
        }
    }

    #[test]
    fn with_nulls(){
        let mut pool = Pool::new();
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let db  = pool.db(db_url).unwrap();
        let rows:Result<Rows, DbError> = (&db).execute_sql_with_return("select 'rust'::TEXT AS name, NULL::TEXT AS schedule, NULL::TEXT AS specialty from actor", &[]);
        println!("columns: {:#?}", rows);
        assert!(rows.is_ok());
        if let Ok(rows) = rows {
            for row in rows.iter(){
                println!("row {:?}", row);
                let name:Result<Option<String>, _> = row.get("name");
                println!("name: {:?}", name);
                assert_eq!(name.unwrap().unwrap(), "rust");

                let schedule:Result<Option<String>, _> = row.get("schedule");
                println!("schedule: {:?}", schedule);
                assert_eq!(schedule.unwrap(), None);

                let specialty: Result<Option<String>, _> = row.get("specialty");
                println!("specialty: {:?}", specialty);
                assert_eq!(specialty.unwrap(), None);
            }
        }
    }


}

#[derive(Debug)]
pub enum PostgresError{
    GenericError(String, postgres::Error),
    SqlError(postgres::Error, String),
    ConvertStringToCharError(String),
    FromUtf8Error(FromUtf8Error),
    ConvertNumericToBigDecimalError,
    PoolInitializationError(r2d2::Error)
}

impl From<postgres::Error> for PostgresError {
    fn from(e: postgres::Error) -> Self {
        PostgresError::GenericError("From conversion".into(), e)
    }
}

impl From<r2d2::Error> for PostgresError {
    fn from(e: r2d2::Error) -> Self {
        PostgresError::PoolInitializationError(e)
    }
}


impl Error for PostgresError {
    fn description(&self) -> &str{
        "postgres error"
    }

    fn cause(&self) -> Option<&Error> { 
        None
    }
}

impl fmt::Display for PostgresError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#?}", self)
    }
}
