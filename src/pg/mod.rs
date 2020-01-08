use self::{
    interval::PgInterval,
    numeric::PgNumeric,
};
use crate::{
    error::PlatformError,
    table::SchemaContent,
    users::{
        Role,
        User,
    },
    DbError,
    Table,
    TableName,
    Value,
    *,
};
use base64;
use bigdecimal::BigDecimal;
use geo::Point;
use log::*;
use postgres::{
    self,
    types::{
        self,
        FromSql,
        IsNull,
        ToSql,
        Type,
    },
};
use postgres_shared::types::{
    Kind,
    Kind::Enum,
};
use r2d2::{
    self,
    ManageConnection,
};
use r2d2_postgres::{
    self,
    TlsMode,
};
use rustorm_dao::{
    value::Array,
    Interval,
    Rows,
};
use serde_json;
use std::{
    error::Error,
    fmt,
    string::FromUtf8Error,
};
use tree_magic;

mod column_info;
#[allow(unused)]
mod interval;
mod numeric;
mod table_info;

pub fn init_pool(
    db_url: &str,
) -> Result<r2d2::Pool<r2d2_postgres::PostgresConnectionManager>, PostgresError> {
    test_connection(db_url)?;
    let manager = r2d2_postgres::PostgresConnectionManager::new(db_url, TlsMode::None)?;
    //let manager = r2d2_postgres::PostgresConnectionManager::new(db_url, get_tls())?;
    let pool = r2d2::Pool::new(manager)?;
    Ok(pool)
}


pub fn test_connection(db_url: &str) -> Result<(), PostgresError> {
    let manager = r2d2_postgres::PostgresConnectionManager::new(db_url, TlsMode::None)?;
    //let manager = r2d2_postgres::PostgresConnectionManager::new(db_url, get_tls())?;
    let mut conn = manager.connect()?;
    manager.is_valid(&mut conn)?;
    Ok(())
}

pub struct PostgresDB(pub r2d2::PooledConnection<r2d2_postgres::PostgresConnectionManager>);

impl Database for PostgresDB {
    fn execute_sql_with_return(&mut self, sql: &str, param: &[&Value]) -> Result<Rows, DbError> {
        let stmt = self.0.prepare(&sql);
        match stmt {
            Ok(stmt) => {
                let pg_values = to_pg_values(param);
                let sql_types = to_sql_types(&pg_values);
                let rows = stmt.query(&sql_types);
                match rows {
                    Ok(rows) => {
                        let columns = rows.columns();
                        let column_names: Vec<String> =
                            columns.iter().map(|c| c.name().to_string()).collect();
                        let mut records = Rows::new(column_names);
                        for r in rows.iter() {
                            let mut record: Vec<Value> = vec![];
                            for (i, column) in columns.iter().enumerate() {
                                let value: Option<Result<OwnedPgValue, postgres::Error>> =
                                    r.get_opt(i);
                                match value {
                                    Some(value) => {
                                        match value {
                                            Ok(value) => record.push(value.0),
                                            Err(e) => {
                                                //info!("Row {:?}", r);
                                                info!("column {:?} index: {}", column, i);
                                                let msg = format!(
                                                    "Error converting column {:?} at index {}",
                                                    column, i
                                                );
                                                return Err(DbError::PlatformError(
                                                    PlatformError::PostgresError(
                                                        PostgresError::GenericError(msg, e),
                                                    ),
                                                ));
                                            }
                                        }
                                    }
                                    None => {
                                        record.push(Value::Nil); // Note: this is important to not mess the spacing of records
                                    }
                                }
                            }
                            records.push(record);
                        }
                        Ok(records)
                    }
                    Err(e) => {
                        Err(DbError::PlatformError(PlatformError::PostgresError(
                            PostgresError::SqlError(e, sql.to_string()),
                        )))
                    }
                }
            }
            Err(e) => {
                Err(DbError::PlatformError(PlatformError::PostgresError(
                    PostgresError::SqlError(e, sql.to_string()),
                )))
            }
        }
    }

    fn get_table(&mut self, table_name: &TableName) -> Result<Table, DbError> {
        table_info::get_table(&mut *self, table_name)
    }

    fn get_all_tables(&mut self) -> Result<Vec<Table>, DbError> {
        table_info::get_all_tables(&mut *self)
    }

    fn get_grouped_tables(&mut self) -> Result<Vec<SchemaContent>, DbError> {
        table_info::get_organized_tables(&mut *self)
    }

    /// get the list of database users
    fn get_users(&mut self) -> Result<Vec<User>, DbError> {
        let sql = "SELECT oid::int AS sysid,
               rolname AS username,
               rolsuper AS is_superuser,
               rolinherit AS is_inherit,
               rolcreaterole AS can_create_role,
               rolcreatedb AS can_create_db,
               rolcanlogin AS can_login,
               rolreplication AS can_do_replication,
               rolbypassrls AS can_bypass_rls,
               CASE WHEN rolconnlimit < 0 THEN NULL
                    ELSE rolconnlimit END AS conn_limit,
               '*************' AS password,
               CASE WHEN rolvaliduntil = 'infinity'::timestamp THEN NULL
                   ELSE rolvaliduntil
                   END AS valid_until
               FROM pg_authid";
        let rows: Result<Rows, DbError> = self.execute_sql_with_return(&sql, &[]);

        rows.map(|rows| {
            rows.iter()
                .map(|row| {
                    User {
                        sysid: row.get("sysid").expect("sysid"),
                        username: row.get("username").expect("username"),
                        password: row.get("password").expect("password"),
                        is_superuser: row.get("is_superuser").expect("is_superuser"),
                        is_inherit: row.get("is_inherit").expect("is_inherit"),
                        can_create_db: row.get("can_create_db").expect("can_create_db"),
                        can_create_role: row.get("can_create_role").expect("can_create_role"),
                        can_login: row.get("can_login").expect("can_login"),
                        can_do_replication: row
                            .get("can_do_replication")
                            .expect("can_do_replication"),
                        can_bypass_rls: row.get("can_bypass_rls").expect("can_bypass_rls"),
                        valid_until: row.get("valid_until").expect("valid_until"),
                        conn_limit: row.get("conn_limit").expect("conn_limit"),
                    }
                })
                .collect()
        })
    }

    /// get the list of roles for this user
    fn get_roles(&mut self, username: &str) -> Result<Vec<Role>, DbError> {
        let sql = "SELECT
            (SELECT rolname FROM pg_roles WHERE oid = m.roleid) AS role_name
            FROM pg_auth_members m
            LEFT JOIN pg_roles
            ON m.member = pg_roles.oid
            WHERE pg_roles.rolname = $1
        ";
        self.execute_sql_with_return(&sql, &[&username.to_value()])
            .map(|rows| {
                rows.iter()
                    .map(|row| {
                        Role {
                            role_name: row.get("role_name").expect("role_name"),
                        }
                    })
                    .collect()
            })
    }

    fn get_database_name(&mut self) -> Result<Option<DatabaseName>, DbError> {
        let sql = "SELECT current_database() AS name,
                        description FROM pg_database
                        LEFT JOIN pg_shdescription ON objoid = pg_database.oid
                        WHERE datname = current_database()";
        let mut database_names: Vec<Option<DatabaseName>> =
            self.execute_sql_with_return(&sql, &[]).map(|rows| {
                rows.iter()
                    .map(|row| {
                        row.get_opt("name").expect("must not error").map(|name| {
                            DatabaseName {
                                name,
                                description: None,
                            }
                        })
                    })
                    .collect()
            })?;

        if database_names.len() > 0 {
            Ok(database_names.remove(0))
        } else {
            Ok(None)
        }
    }
}


fn to_pg_values<'a>(values: &[&'a Value]) -> Vec<PgValue<'a>> {
    values.iter().map(|v| PgValue(v)).collect()
}

fn to_sql_types<'a>(values: &'a [PgValue]) -> Vec<&'a dyn ToSql> {
    let mut sql_types = vec![];
    for v in values.iter() {
        sql_types.push(&*v as &dyn ToSql);
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

impl<'a> ToSql for PgValue<'a> {
    to_sql_checked!();

    fn to_sql(
        &self,
        ty: &Type,
        out: &mut Vec<u8>,
    ) -> Result<IsNull, Box<dyn Error + 'static + Sync + Send>> {
        match *self.0 {
            Value::Bool(ref v) => v.to_sql(ty, out),
            Value::Tinyint(ref v) => v.to_sql(ty, out),
            Value::Smallint(ref v) => v.to_sql(ty, out),
            Value::Int(ref v) => v.to_sql(ty, out),
            Value::Bigint(ref v) => v.to_sql(ty, out),
            Value::Float(ref v) => v.to_sql(ty, out),
            Value::Double(ref v) => v.to_sql(ty, out),
            Value::Blob(ref v) => v.to_sql(ty, out),
            Value::ImageUri(ref _v) => {
                panic!("ImageUri is only used for reading data from DB, not inserting into DB")
            }
            Value::Char(ref v) => v.to_string().to_sql(ty, out),
            Value::Text(ref v) => v.to_sql(ty, out),
            Value::Uuid(ref v) => v.to_sql(ty, out),
            Value::Date(ref v) => v.to_sql(ty, out),
            Value::Timestamp(ref v) => v.to_sql(ty, out),
            Value::DateTime(ref v) => v.to_sql(ty, out),
            Value::Time(ref v) => v.to_sql(ty, out),
            Value::Interval(ref _v) => panic!("storing interval in DB is not supported"),
            Value::BigDecimal(ref v) => {
                let numeric: PgNumeric = v.into();
                numeric.to_sql(ty, out)
            }
            Value::Json(ref v) => v.to_sql(ty, out),
            Value::Point(ref v) => v.to_sql(ty, out),
            Value::Array(ref v) => {
                match *v {
                    Array::Text(ref av) => av.to_sql(ty, out),
                    Array::Int(ref av) => av.to_sql(ty, out),
                    Array::Float(ref av) => av.to_sql(ty, out),
                }
            }
            Value::Nil => Ok(IsNull::Yes),
        }
    }

    fn accepts(_ty: &Type) -> bool { true }
}

impl FromSql for OwnedPgValue {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        macro_rules! match_type {
            ($variant:ident) => {
                FromSql::from_sql(ty, raw).map(|v| OwnedPgValue(Value::$variant(v)))
            };
        }
        let kind = ty.kind();
        match *kind {
            Enum(_) => match_type!(Text),
            Kind::Array(ref array_type) => {
                let array_type_kind = array_type.kind();
                match *array_type_kind {
                    Enum(_) => {
                        FromSql::from_sql(ty, raw)
                            .map(|v| OwnedPgValue(Value::Array(Array::Text(v))))
                    }
                    _ => {
                        match *ty {
                            types::TEXT_ARRAY | types::NAME_ARRAY | types::VARCHAR_ARRAY => {
                                FromSql::from_sql(ty, raw)
                                    .map(|v| OwnedPgValue(Value::Array(Array::Text(v))))
                            }
                            types::INT4_ARRAY => {
                                FromSql::from_sql(ty, raw)
                                    .map(|v| OwnedPgValue(Value::Array(Array::Int(v))))
                            }
                            types::FLOAT4_ARRAY => {
                                FromSql::from_sql(ty, raw)
                                    .map(|v| OwnedPgValue(Value::Array(Array::Float(v))))
                            }
                            _ => panic!("Array type {:?} is not yet covered", array_type),
                        }
                    }
                }
            }
            Kind::Simple => {
                match *ty {
                    types::BOOL => match_type!(Bool),
                    types::INT2 => match_type!(Smallint),
                    types::INT4 => match_type!(Int),
                    types::INT8 => match_type!(Bigint),
                    types::FLOAT4 => match_type!(Float),
                    types::FLOAT8 => match_type!(Double),
                    types::TEXT | types::VARCHAR | types::NAME | types::UNKNOWN => {
                        match_type!(Text)
                    }
                    types::TS_VECTOR => {
                        let text = String::from_utf8(raw.to_owned());
                        match text {
                            Ok(text) => Ok(OwnedPgValue(Value::Text(text))),
                            Err(e) => Err(Box::new(PostgresError::FromUtf8Error(e))),
                        }
                    }
                    types::BPCHAR => {
                        let v: Result<String, _> = FromSql::from_sql(&types::TEXT, raw);
                        match v {
                            Ok(v) => {
                                // TODO: Need to unify char and character array in one Value::Text
                                // variant to simplify handling them in some column
                                if v.chars().count() == 1 {
                                    Ok(OwnedPgValue(Value::Char(v.chars().next().unwrap())))
                                } else {
                                    FromSql::from_sql(ty, raw).map(|v: String| {
                                        let value_string: String = v.trim_end().to_string();
                                        OwnedPgValue(Value::Text(value_string))
                                    })
                                }
                            }
                            Err(e) => Err(e),
                        }
                    }
                    types::UUID => match_type!(Uuid),
                    types::DATE => match_type!(Date),
                    types::TIMESTAMPTZ | types::TIMESTAMP => match_type!(Timestamp),
                    types::TIME | types::TIMETZ => match_type!(Time),
                    types::BYTEA => {
                        let mime_type = tree_magic::from_u8(raw);
                        info!("mime_type: {}", mime_type);
                        let bytes: Vec<u8> = FromSql::from_sql(ty, raw).unwrap();
                        //assert_eq!(raw, &*bytes);
                        let base64 = base64::encode_config(&bytes, base64::MIME);
                        match &*mime_type {
                            "image/jpeg" | "image/png" => {
                                Ok(OwnedPgValue(Value::ImageUri(format!(
                                    "data:{};base64,{}",
                                    mime_type, base64
                                ))))
                            }
                            _ => match_type!(Blob),
                        }
                    }
                    types::NUMERIC => {
                        let numeric: PgNumeric = FromSql::from_sql(ty, raw)?;
                        let bigdecimal = BigDecimal::from(numeric);
                        Ok(OwnedPgValue(Value::BigDecimal(bigdecimal)))
                    }
                    types::JSON | types::JSONB => {
                        let value: serde_json::Value = FromSql::from_sql(ty, raw)?;
                        let text = serde_json::to_string(&value).unwrap();
                        Ok(OwnedPgValue(Value::Json(text)))
                    }
                    types::INTERVAL => {
                        let pg_interval: PgInterval = FromSql::from_sql(ty, raw)?;
                        let interval = Interval::new(
                            pg_interval.microseconds,
                            pg_interval.days,
                            pg_interval.months,
                        );
                        Ok(OwnedPgValue(Value::Interval(interval)))
                    }
                    types::POINT => {
                        let p: Point<f64> = FromSql::from_sql(ty, raw)?;
                        Ok(OwnedPgValue(Value::Point(p)))
                    }
                    types::INET => {
                        info!("inet raw:{:?}", raw);
                        match_type!(Text)
                    }
                    _ => panic!("unable to convert from {:?}", ty),
                }
            }
            _ => panic!("not yet handling this kind: {:?}", kind),
        }
    }

    fn accepts(_ty: &Type) -> bool { true }

    fn from_sql_null(_ty: &Type) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(OwnedPgValue(Value::Nil))
    }

    fn from_sql_nullable(
        ty: &Type,
        raw: Option<&[u8]>,
    ) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match raw {
            Some(raw) => Self::from_sql(ty, raw),
            None => Self::from_sql_null(ty),
        }
    }
}

#[derive(Debug)]
pub enum PostgresError {
    GenericError(String, postgres::Error),
    SqlError(postgres::Error, String),
    ConvertStringToCharError(String),
    FromUtf8Error(FromUtf8Error),
    ConvertNumericToBigDecimalError,
    PoolInitializationError(r2d2::Error),
}

impl From<postgres::Error> for PostgresError {
    fn from(e: postgres::Error) -> Self { PostgresError::GenericError("From conversion".into(), e) }
}

impl From<r2d2::Error> for PostgresError {
    fn from(e: r2d2::Error) -> Self { PostgresError::PoolInitializationError(e) }
}

impl Error for PostgresError {}

impl fmt::Display for PostgresError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result { write!(f, "{:#?}", self) }
}

#[cfg(test)]
mod test {

    use crate::{
        pool::*,
        Pool,
        *,
    };
    use log::*;
    use postgres::Connection;
    use std::ops::Deref;


    #[test]
    fn test_character_array_data_type() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let mut dm = pool.dm(db_url).unwrap();
        let sql = format!("SELECT language_id, name FROM language",);
        let languages: Result<Rows, DbError> = dm.execute_sql_with_return(&sql, &[]);
        println!("languages: {:#?}", languages);
        assert!(languages.is_ok());
    }

    #[test]
    fn test_ts_vector() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let mut dm = pool.dm(db_url).unwrap();
        let sql = format!("SELECT film_id, title, fulltext::text FROM film LIMIT 40",);
        let films: Result<Rows, DbError> = dm.execute_sql_with_return(&sql, &[]);
        println!("film: {:#?}", films);
        assert!(films.is_ok());
    }
    #[test]
    fn connect_test_query() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let conn = pool.connect(db_url);
        assert!(conn.is_ok());
        let conn: PooledConn = conn.unwrap();
        match conn {
            PooledConn::PooledPg(ref pooled_pg) => {
                let rows = pooled_pg.query("select 42, 'life'", &[]).unwrap();
                for row in rows.iter() {
                    let n: i32 = row.get(0);
                    let l: String = row.get(1);
                    assert_eq!(n, 42);
                    assert_eq!(l, "life");
                }
            }
            #[cfg(any(feature = "with-sqlite", feature = "with-mysql"))]
            _ => unreachable!(),
        }
    }
    #[test]
    fn connect_test_query_explicit_deref() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let conn = pool.connect(db_url);
        assert!(conn.is_ok());
        let conn: PooledConn = conn.unwrap();
        match conn {
            PooledConn::PooledPg(ref pooled_pg) => {
                let c: &Connection = pooled_pg.deref(); //explicit deref here
                let rows = c.query("select 42, 'life'", &[]).unwrap();
                for row in rows.iter() {
                    let n: i32 = row.get(0);
                    let l: String = row.get(1);
                    assert_eq!(n, 42);
                    assert_eq!(l, "life");
                }
            }
            #[cfg(any(feature = "with-sqlite", feature = "with-mysql"))]
            _ => unreachable!(),
        }
    }
    #[test]
    fn test_unknown_type() {
        let mut pool = Pool::new();
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let mut db = pool.db(db_url).unwrap();
        let values: Vec<Value> = vec!["hi".into(), true.into(), 42.into(), 1.0.into()];
        let bvalues: Vec<&Value> = values.iter().collect();
        let rows: Result<Rows, DbError> = db.execute_sql_with_return(
            "select 'Hello', $1::TEXT, $2::BOOL, $3::INT, $4::FLOAT",
            &bvalues,
        );
        info!("rows: {:#?}", rows);
        assert!(rows.is_ok());
    }
    #[test]
    // only text can be inferred to UNKNOWN types
    fn test_unknown_type_i32_f32() {
        let mut pool = Pool::new();
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let mut db = pool.db(db_url).unwrap();
        let values: Vec<Value> = vec![42.into(), 1.0.into()];
        let bvalues: Vec<&Value> = values.iter().collect();
        let rows: Result<Rows, DbError> = db.execute_sql_with_return("select $1, $2", &bvalues);
        info!("rows: {:#?}", rows);
        assert!(!rows.is_ok());
    }

    #[test]
    fn using_values() {
        let mut pool = Pool::new();
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let mut db = pool.db(db_url).unwrap();
        let values: Vec<Value> = vec!["hi".into(), true.into(), 42.into(), 1.0.into()];
        let bvalues: Vec<&Value> = values.iter().collect();
        let rows: Result<Rows, DbError> = db.execute_sql_with_return(
            "select 'Hello'::TEXT, $1::TEXT, $2::BOOL, $3::INT, $4::FLOAT",
            &bvalues,
        );
        info!("columns: {:#?}", rows);
        assert!(rows.is_ok());
        if let Ok(rows) = rows {
            for row in rows.iter() {
                info!("row {:?}", row);
                let v4: Result<f64, _> = row.get("float8");
                assert_eq!(v4.unwrap(), 1.0f64);

                let v3: Result<i32, _> = row.get("int4");
                assert_eq!(v3.unwrap(), 42i32);

                let hi: Result<String, _> = row.get("text");
                assert_eq!(hi.unwrap(), "hi");

                let b: Result<bool, _> = row.get("bool");
                assert_eq!(b.unwrap(), true);
            }
        }
    }

    #[test]
    fn with_nulls() {
        let mut pool = Pool::new();
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let mut db = pool.db(db_url).unwrap();
        let rows:Result<Rows, DbError> = db.execute_sql_with_return("select 'rust'::TEXT AS name, NULL::TEXT AS schedule, NULL::TEXT AS specialty from actor", &[]);
        info!("columns: {:#?}", rows);
        assert!(rows.is_ok());
        if let Ok(rows) = rows {
            for row in rows.iter() {
                info!("row {:?}", row);
                let name: Result<Option<String>, _> = row.get("name");
                info!("name: {:?}", name);
                assert_eq!(name.unwrap().unwrap(), "rust");

                let schedule: Result<Option<String>, _> = row.get("schedule");
                info!("schedule: {:?}", schedule);
                assert_eq!(schedule.unwrap(), None);

                let specialty: Result<Option<String>, _> = row.get("specialty");
                info!("specialty: {:?}", specialty);
                assert_eq!(specialty.unwrap(), None);
            }
        }
    }

    #[test]
    fn test_get_users() {
        let mut pool = Pool::new();
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let mut em = pool.em(db_url).unwrap();
        let users = em.get_users();
        info!("users: {:#?}", users);
        assert!(users.is_ok());
    }
}
