use r2d2;
use r2d2_sqlite;
use rusqlite;
use crate::DbError;
use crate::error::PlatformError;
use crate::Database;
use crate::{Value,Rows};
use crate::Table;
use crate::EntityManager;
use crate::TableName;
use crate::table::SchemaContent;
use r2d2::ManageConnection;
use crate::FromDao;
use crate::types::SqlType;
use crate::common;
use crate::ColumnName;
use crate::table::{TableKey,ForeignKey, Key};
use crate::column::{Column, ColumnConstraint, Literal, ColumnSpecification, Capacity};
use crate::util;
use uuid::Uuid;
use crate::DatabaseName;
use crate::users::User;
use crate::users::Role;
use log::*;

pub fn init_pool(db_url: &str) -> Result<r2d2::Pool<r2d2_sqlite::SqliteConnectionManager>, SqliteError> {
    info!("initializing pool: {}", db_url);
    let manager = r2d2_sqlite::SqliteConnectionManager::file(db_url);
    let pool = r2d2::Pool::new(manager)?;
    Ok(pool)
}

pub fn test_connection(db_url: &str) -> Result<(), SqliteError> {
    let manager = r2d2_sqlite::SqliteConnectionManager::file(db_url);
    let mut conn = manager.connect()?;
    manager.is_valid(&mut conn)?;
    Ok(())
}

pub struct SqliteDB(pub r2d2::PooledConnection<r2d2_sqlite::SqliteConnectionManager>);

fn to_sq_value(val: &Value) -> rusqlite::types::Value {
    use num_traits::ToPrimitive;
    match *val{
        Value::Text(ref v) => rusqlite::types::Value::Text(v.to_owned()),
        Value::Bool(v) => {
            rusqlite::types::Value::Integer(if v{1}else{0})
        }
        Value::Tinyint(v) => {
            rusqlite::types::Value::Integer(v as i64)
        }
        Value::Smallint(v) => {
            rusqlite::types::Value::Integer(v as i64)
        }
        Value::Int(v) => {
            rusqlite::types::Value::Integer(v as i64)
        }
        Value::Bigint(v) => {
            rusqlite::types::Value::Integer(v as i64)
        }

        Value::Float(v) => {
            rusqlite::types::Value::Real(v as f64)
        }
        Value::Double(v) => {
            rusqlite::types::Value::Real(v as f64)
        }
        Value::BigDecimal(ref v) => {
            match v.to_f64(){
                Some(v) => rusqlite::types::Value::Real(v as f64),
                None => panic!("unable to convert bigdecimal"),
            }
        }
        Value::Blob(ref v) => rusqlite::types::Value::Blob(v.clone()),
        Value::ImageUri(ref v) => rusqlite::types::Value::Text(v.clone()),
        Value::Char(v) => rusqlite::types::Value::Text(format!("{}", v)),
        Value::Json(ref v) => rusqlite::types::Value::Text(v.clone()),
        Value::Uuid(ref v) => rusqlite::types::Value::Text(v.to_string()),
        Value::Date(ref v) =>  rusqlite::types::Value::Text(v.to_string()),
        Value::Nil => rusqlite::types::Value::Null,
        _ => panic!("not yet handled: {:?}", val),
    }
}

fn to_sq_values(params: &[&Value]) -> Vec<rusqlite::types::Value> {
    let mut sql_values = Vec::with_capacity(params.len());
    for param in params{
        let sq_val = to_sq_value(param);
        sql_values.push(sq_val);
    }
    sql_values
}


impl Database for SqliteDB{

    fn execute_sql_with_return(&self, sql: &str, params: &[&Value]) -> Result<Rows, DbError> {
        info!("executing sql: {}", sql);
        info!("params: {:?}", params);
        let stmt = self.0.prepare(&sql);

        let column_names = if let Ok(ref stmt) = stmt {
            stmt.column_names()
        }else{
            vec![]
        };
        let column_names: Vec<String> = column_names
            .iter()
            .map(|c| c.to_string())
            .collect();

        match stmt{
            Ok(mut stmt) => {
                let sq_values = to_sq_values(params);
                let column_count = stmt.column_count();
                 let mut records = Rows::new(column_names);
                 if let Ok(mut rows) = stmt.query(sq_values){
                     while let Some(row) = rows.next()?{
                         let mut record: Vec<Value>  = vec![];
                         for i in 0..column_count{
                             let raw = row.get(i);
                             if let Ok(raw) = raw{
                                 let value = match raw{
                                    rusqlite::types::Value::Blob(v) => Value::Blob(v),
                                    rusqlite::types::Value::Real(v) => Value::Double(v),
                                    rusqlite::types::Value::Integer(v) => Value::Bigint(v),
                                    rusqlite::types::Value::Text(v) => Value::Text(v),
                                    rusqlite::types::Value::Null => Value::Nil,
                                };
                                record.push(value);
                             }
                         }
                        records.push(record);
                     }
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


    #[allow(unused_variables)]
    fn get_table(&self, em: &EntityManager, table_name: &TableName) -> Result<Table, DbError> {
        #[derive(Debug)]
        struct ColumnSimple{
            name: String,
            data_type: String,
            not_null: bool,
            default: Option<String>,
            pk: bool
        }
        impl ColumnSimple{

            fn to_column(&self, table_name: &TableName) -> Column {
               Column{
                   table: table_name.clone(),
                   name: ColumnName::from(&self.name),
                   comment: None,
                   specification: self.to_column_specification(),
                   stat: None,
               }
            }

            fn to_column_specification(&self) -> ColumnSpecification {
                let (sql_type, capacity) = self.get_sql_type_capacity();
                ColumnSpecification{
                    sql_type,
                    capacity,
                    constraints: self.to_column_constraints(),
                }
            }

            fn to_column_constraints(&self) -> Vec<ColumnConstraint> {
                let (sql_type, _) = self.get_sql_type_capacity();
                let mut constraints = vec![];
                if self.not_null{
                    constraints.push(ColumnConstraint::NotNull);
                }
                if let Some(ref default) = self.default{
                    let ic_default = default.to_lowercase();
                    let constraint = if ic_default == "null" {
                        ColumnConstraint::DefaultValue(Literal::Null)
                    }
                    else if ic_default.starts_with("nextval"){
                        ColumnConstraint::AutoIncrement
                    }
                    else {
                        let literal =  match sql_type {
                            SqlType::Bool => {
                                let v: bool = default.parse().unwrap();
                                Literal::Bool(v)
                            }
                            SqlType::Int
                                | SqlType::Smallint
                                | SqlType::Tinyint
                                | SqlType::Bigint => {
                                    let v: Result<i64,_> = default.parse();
                                    match v{
                                        Ok(v) => Literal::Integer(v),
                                        Err(e) => panic!("error parsing to integer: {} error: {}", default, e)
                                    }
                                },
                            SqlType::Float
                                | SqlType::Double
                                | SqlType::Real
                                | SqlType::Numeric => {
                                    // some defaults have cast type example: (0)::numeric
                                    let splinters = util::maybe_trim_parenthesis(&default).split("::").collect::<Vec<&str>>();
                                    let default_value = util::maybe_trim_parenthesis(splinters[0]);
                                    if default_value.to_lowercase() == "null" {
                                        Literal::Null
                                    }
                                    else{
                                        match util::eval_f64(default){
                                            Ok(val) => Literal::Double(val),
                                            Err(e) => panic!("unable to evaluate default value expression: {}, error: {}", default, e),
                                        }
                                    }

                                }
                            SqlType::Uuid => {
                                if ic_default == "uuid_generate_v4()"{
                                   Literal::UuidGenerateV4
                                }
                                else{
                                    let v: Result<Uuid,_> = Uuid::parse_str(&default);
                                    match v{
                                        Ok(v) => Literal::Uuid(v),
                                        Err(e) => panic!("error parsing to uuid: {} error: {}", default, e)
                                    }
                                }
                            }
                            SqlType::Timestamp
                                | SqlType::TimestampTz
                                => {
                                    if ic_default == "now()" || ic_default == "timezone('utc'::text, now())"
                                        || ic_default == "current_timestamp"
                                    {
                                        Literal::CurrentTimestamp
                                    }
                                    else{
                                        panic!("timestamp other than now is not covered, got: {}", ic_default)
                                    }
                                }
                            SqlType::Date => {
                                // timestamp converted to text then converted to date
                                // is equivalent to today()
                                if ic_default == "today()" || ic_default == "now()" || ic_default =="('now'::text)::date" {
                                    Literal::CurrentDate
                                }
                                else{
                                    panic!("date other than today, now is not covered in {:?}", self)
                                }
                            }
                            SqlType::Varchar
                                | SqlType::Char
                                | SqlType::Tinytext
                                | SqlType::Mediumtext
                                | SqlType::Text
                                    => Literal::String(default.to_owned()),
                            SqlType::Enum(_name, _choices) => Literal::String(default.to_owned()),
                            _ => panic!("not convered: {:?}", sql_type),
                        };
                        ColumnConstraint::DefaultValue(literal)
                    };
                    constraints.push(constraint);

                }
                constraints
            }

            fn get_sql_type_capacity(&self) -> (SqlType, Option<Capacity>) {
                let (dtype,capacity) = common::extract_datatype_with_capacity(&self.data_type);
                let sql_type = match &*dtype{
                    "int" | "integer" => SqlType::Int,
                    "smallint" => SqlType::Smallint,
                    "varchar" => SqlType::Text,
                    "character varying" => SqlType::Text,
                    "decimal" => SqlType::Double,
                    "timestamp" => SqlType::Timestamp,
                    "numeric" => SqlType::Numeric,
                    "char" => match capacity{
                        None => SqlType::Char,
                        Some(Capacity::Limit(1)) => SqlType::Char,
                        Some(_) => SqlType::Varchar,
                    }
                    "blob" => SqlType::Blob,
                    "" => SqlType::Text,
                    _ => {
                        if dtype.contains("text") {
                            SqlType::Text
                        }
                        else{
                            panic!("not yet handled: {:?}", dtype)
                        }
                    }
                };
                (sql_type, capacity)
            }
        }
        macro_rules! unwrap_ok_some {
            ($var: ident ) => {
                match $var {
                    Ok($var) => match $var{
                        Some($var) => $var,
                        None => panic!("expecting {} to have a value", stringify!($var))
                    }
                    Err(_e) => {
                        panic!("expecting {} to be not error", stringify!($var))
                    }
                }
            }
        }
        let sql = format!("PRAGMA table_info({});", table_name.complete_name());
        let result = self.execute_sql_with_return(&sql, &vec![])?;
        let mut primary_columns = vec![];
        let mut columns = vec![];
        for dao in result.iter(){
            let name:Result<Option<String>,_> = dao.get("name");
            let name = unwrap_ok_some!(name);
            let data_type:Result<Option<String>,_> = dao.get("type");
            let data_type = unwrap_ok_some!(data_type).to_lowercase();
            let not_null: Result<Option<i64>,_> = dao.get("notnull");
            let not_null = unwrap_ok_some!(not_null) != 0;
            let pk: Result<Option<i64>,_> = dao.get("pk");
            let pk = unwrap_ok_some!(pk) != 0;
            if pk{
                primary_columns.push(ColumnName::from(&name));
            }
            let default = dao.0.get("dflt_value")
                    .map(|v|
                         match *v{
                             Value::Text(ref v) => v.to_owned(),
                             Value::Nil => "null".to_string(),
                             _ => panic!("Expecting a text value, got: {:?}", v)
                    });
            let simple = ColumnSimple{
                name,
                data_type,
                default,
                pk,
                not_null
            };
            columns.push(simple.to_column(table_name));
        }
        let primary_key = Key{
            name: None,
            columns: primary_columns
        };
        info!("primary key: {:#?}", primary_key);
        let foreign_keys = get_foreign_keys(em, table_name)?;
        let table_key_foreign:Vec<TableKey> = foreign_keys.into_iter()
                .map(|fk| TableKey::ForeignKey(fk))
                .collect();
        let mut table_keys = vec![
            TableKey::PrimaryKey(primary_key),
        ];
        table_keys.extend(table_key_foreign);
        let table = Table{
            name: table_name.clone(),
            comment: None, // TODO: need to extract comment from the create_sql
            columns: columns,
            is_view: false,
            table_key: table_keys,
        };
        Ok(table)
    }

    fn get_all_tables(&self, em: &EntityManager) -> Result<Vec<Table>, DbError> {
        #[derive(Debug,FromDao)]
        struct TableNameSimple{
            tbl_name: String,
        }
        let sql = "SELECT tbl_name FROM sqlite_master WHERE type IN ('table', 'view')";
        let result: Vec<TableNameSimple> = em.execute_sql_with_return(sql, &[])?;
        let mut tables = vec![];
        for r in result{
            let table_name = TableName::from(&r.tbl_name);
            let table = em.get_table(&table_name)?;
            tables.push(table);
        }
        Ok(tables)
    }
    fn get_grouped_tables(&self, em: &EntityManager) -> Result<Vec<SchemaContent>, DbError> {
        let table_names = get_table_names(em, &"table".to_string())?;
        let view_names = get_table_names(em, &"view".to_string())?;
        let schema_content = SchemaContent {
            schema: "".to_string(),
            tablenames: table_names,
            views: view_names,
        };
        Ok(vec![schema_content])
    }

    /// there are no users in sqlite
    fn get_users(&self, _em: &EntityManager) -> Result<Vec<User>, DbError> {
        Err(DbError::UnsupportedOperation("sqlite doesn't have operatio to extract users".to_string()))
    }

    /// there are not roles in sqlite
    fn get_roles(&self, _em: &EntityManager, _username: &str) -> Result<Vec<Role>, DbError> {
        Err(DbError::UnsupportedOperation("sqlite doesn't have operatio to extract roles".to_string()))
    }

    /// TODO: return the filename if possible
    fn get_database_name(&self, _em: &EntityManager) -> Result<Option<DatabaseName>, DbError> {
        Ok(None)
    }
}

fn get_table_names(em: &EntityManager, kind: &str) -> Result<Vec<TableName>, DbError> {
    #[derive(Debug,FromDao)]
    struct TableNameSimple{
        tbl_name: String,
    }
    let sql = "SELECT tbl_name FROM sqlite_master WHERE type = ?";
    let result: Vec<TableNameSimple> = em.execute_sql_with_return(sql, &[&kind.to_string()])?;
    let mut table_names = vec![];
    for r in result{
        let table_name = TableName::from(&r.tbl_name);
        table_names.push(table_name);
    }
    Ok(table_names)
}

/// get the foreign keys of table
fn get_foreign_keys(em: &EntityManager, table: &TableName) -> Result<Vec<ForeignKey>, DbError> {
    let sql = format!("PRAGMA foreign_key_list({});", table.complete_name());
    #[derive(Debug,FromDao)]
    struct ForeignSimple{
        id: i64,
        table: String,
        from: String,
        to: String,
    }
    let result: Vec<ForeignSimple> = em.execute_sql_with_return(&sql, &vec![])?;
    let mut foreign_tables:Vec<(i64, TableName)> = result.iter().map(|f| (f.id, TableName::from(&f.table)) ).collect();
    foreign_tables.dedup();
    let mut foreign_keys = Vec::with_capacity(foreign_tables.len());
    for (id,foreign_table) in foreign_tables{
        let foreigns:Vec<&ForeignSimple> = result.iter().filter(|f| f.id == id).collect();
        let (local_columns, referred_columns):(Vec<ColumnName>, Vec<ColumnName>) = foreigns.iter().map(|f| (ColumnName::from(&f.from), ColumnName::from(&f.to))).unzip();
        let foreign_key = ForeignKey{
            name: None,
            columns: local_columns,
            foreign_table,
            referred_columns,
        };
        foreign_keys.push(foreign_key);
    }
    Ok(foreign_keys)
}



#[derive(Debug)]
pub enum SqliteError{
    GenericError(rusqlite::Error),
    SqlError(rusqlite::Error, String),
    PoolInitializationError(r2d2::Error),
}

impl From<r2d2::Error> for SqliteError{
    fn from(e: r2d2::Error) -> Self {
        SqliteError::PoolInitializationError(e)
    }
}

impl From<rusqlite::Error> for SqliteError {
    fn from(e: rusqlite::Error) -> Self {
        SqliteError::GenericError(e)
    }
}


#[cfg(test)]
mod test{
    use super::*;
    use crate::Pool;
    use crate::pool;

    use crate::column::Literal::Null;
    use crate::column::ColumnConstraint::{NotNull,DefaultValue};
    use crate::types::SqlType::{Text,Int,Timestamp};
    use crate::column::Capacity::Limit;

    #[test]
    fn test_get_all_tables(){
        let db_url = "sqlite://sakila.db";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let all_tables = em.get_all_tables();
        assert!(all_tables.is_ok());
        let all_tables = all_tables.unwrap();
        assert_eq!(all_tables.len(), 22);
    }

    #[test]
    fn test_get_group_table(){
        let db_url = "sqlite://sakila.db";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let schema_content = em.get_grouped_tables();
        assert!(schema_content.is_ok());
        let schema_content = schema_content.unwrap();
        let schema_content = &schema_content[0];
        assert_eq!(schema_content.tablenames.len(), 17);
        assert_eq!(schema_content.views.len(), 5);
        info!("schema_content: {:#?}", schema_content);
    }

    #[test]
    fn test_conn(){
        let db_url = "sqlite://sakila.db";
        let result = pool::test_connection(db_url);
        info!("result: {:?}", result);
        assert!(result.is_ok());
    }


    #[test]
    fn test_get_table(){
        let db_url = "sqlite://sakila.db";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let film = "film";
        let film_table = TableName::from(film);
        let table = em.get_table(&film_table);
        assert!(table.is_ok());
        let table = table.unwrap();
        info!("table: {:#?}", table);
        assert_eq!(table,
            Table {
                name: TableName::from("film"),
                comment: None,
                columns: vec![
                    Column {
                        table: TableName::from("film"),
                        name: ColumnName::from("film_id"),
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: SqlType::Int,
                            capacity: None,
                            constraints: vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::DefaultValue(
                                    Literal::Null
                                )
                            ]
                        },
                        stat: None
                    },
                    Column {
                        table: TableName::from("film"),
                        name: ColumnName::from("title"),
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: SqlType::Text,
                            capacity: Some(
                                Capacity::Limit( 255)
                            ),
                            constraints: vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::DefaultValue(
                                    Literal::Null
                                )
                            ]
                        },
                        stat: None
                    },
                    Column {
                        table: TableName::from("film"),
                        name: ColumnName::from("description"),
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: SqlType::Text,
                            capacity: None,
                            constraints: vec![
                                ColumnConstraint::DefaultValue(
                                    Literal::Null
                                )
                            ]
                        },
                        stat: None
                    },
                    Column {
                        table: TableName::from("film"),
                        name: ColumnName::from("release_year"),
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: SqlType::Text,
                            capacity: Some(
                                Capacity::Limit( 4)
                            ),
                            constraints: vec![
                                ColumnConstraint::DefaultValue(
                                    Literal::Null
                                )
                            ]
                        },
                        stat: None
                    },
                    Column {
                        table: TableName::from("film"),
                        name: ColumnName::from("language_id"),
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: SqlType::Smallint,
                            capacity: None,
                            constraints: vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::DefaultValue(
                                    Literal::Null
                                )
                            ]
                        },
                        stat: None
                    },
                    Column {
                        table: TableName::from("film"),
                        name: ColumnName::from("original_language_id"),
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: SqlType::Smallint,
                            capacity: None,
                            constraints: vec![
                                ColumnConstraint::DefaultValue(
                                    Literal::Null
                                )
                            ]
                        },
                        stat: None
                    },
                    Column {
                        table: TableName::from("film"),
                        name: ColumnName::from("rental_duration"),
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: SqlType::Smallint,
                            capacity: None,
                            constraints: vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::DefaultValue(
                                    Literal::Integer(3)
                                )
                            ]
                        },
                        stat: None
                    },
                    Column {
                        table: TableName::from("film"),
                        name: ColumnName::from("rental_rate"),
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: SqlType::Double,
                            capacity: Some(
                                Capacity::Range( 4, 2)
                            ),
                            constraints: vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::DefaultValue(
                                    Literal::Double( 4.99)
                                )
                            ]
                        },
                        stat: None
                    },
                    Column {
                        table: TableName::from("film"),
                        name: ColumnName::from("length"),
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: SqlType::Smallint,
                            capacity: None,
                            constraints: vec![
                                ColumnConstraint::DefaultValue(
                                    Literal::Null
                                )
                            ]
                        },
                        stat: None
                    },
                    Column {
                        table: TableName::from("film"),
                        name: ColumnName::from("replacement_cost"),
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: SqlType::Double,
                            capacity: Some(
                                Capacity::Range( 5, 2)
                            ),
                            constraints: vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::DefaultValue(
                                    Literal::Double( 19.99)
                                )
                            ]
                        },
                        stat: None
                    },
                    Column {
                        table: TableName::from("film"),
                        name: ColumnName::from("rating"),
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: SqlType::Text,
                            capacity: Some(
                                Capacity::Limit(
                                    10
                                )
                            ),
                            constraints: vec![
                                ColumnConstraint::DefaultValue(
                                    Literal::String( "\'G\'".to_string())
                                )
                            ]
                        },
                        stat: None
                    },
                    Column {
                        table: TableName::from("film"),
                        name: ColumnName::from("special_features"),
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: SqlType::Text,
                            capacity: Some(
                                Capacity::Limit( 100)
                            ),
                            constraints: vec![
                                ColumnConstraint::DefaultValue(
                                    Literal::Null
                                )
                            ]
                        },
                        stat: None
                    },
                    Column {
                        table: TableName::from("film"),
                        name: ColumnName::from("last_update"),
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: SqlType::Timestamp,
                            capacity: None,
                            constraints: vec![
                                ColumnConstraint::NotNull,
                                ColumnConstraint::DefaultValue(
                                    Literal::Null
                                )
                            ]
                        },
                        stat: None
                    }
                ],
                is_view: false,
                table_key: vec![
                    TableKey::PrimaryKey(
                        Key {
                            name: None,
                            columns: vec![
                                ColumnName::from("film_id")
                            ]
                        }
                    ),
                    TableKey::ForeignKey(
                        ForeignKey {
                            name: None,
                            columns: vec![
                                ColumnName::from("original_language_id"),
                            ],
                            foreign_table: TableName::from("language"),
                            referred_columns: vec![
                                ColumnName::from("language_id"),
                            ]
                        }
                    ),
                    TableKey::ForeignKey(
                        ForeignKey {
                            name: None,
                            columns: vec![
                                ColumnName::from("language_id"),
                            ],
                            foreign_table: TableName::from("language"),
                            referred_columns: vec![
                                ColumnName::from("language_id"),
                            ]
                        }
                    )
                ]
            }
        );
    }

    #[test]
    fn test_get_table2(){
        let db_url = "sqlite://sakila.db";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let table = "actor";
        let table_name = TableName::from(table);
        let table = em.get_table(&table_name);
        assert!(table.is_ok());
        let table = table.unwrap();
        info!("table: {:#?}", table);
        assert_eq!(table,

            Table {
                name: TableName {
                    name: "actor".into(),
                    schema: None,
                    alias: None
                },
                comment: None,
                columns: vec![
                    Column {
                        table: TableName {
                            name: "actor".into(),
                            schema: None,
                            alias: None
                        },
                        name: ColumnName {
                            name: "actor_id".into(),
                            table: None,
                            alias: None
                        },
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: Int,
                            capacity: None,
                            constraints: vec![
                                NotNull,
                                DefaultValue(
                                    Null
                                )
                            ]
                        },
                        stat: None
                    },
                    Column {
                        table: TableName {
                            name: "actor".into(),
                            schema: None,
                            alias: None
                        },
                        name: ColumnName {
                            name: "first_name".into(),
                            table: None,
                            alias: None
                        },
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: Text,
                            capacity: Some(
                                Limit(
                                    45
                                )
                            ),
                            constraints: vec![
                                NotNull,
                                DefaultValue(
                                    Null
                                )
                            ]
                        },
                        stat: None
                    },
                    Column {
                        table: TableName {
                            name: "actor".into(),
                            schema: None,
                            alias: None
                        },
                        name: ColumnName {
                            name: "last_name".into(),
                            table: None,
                            alias: None
                        },
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: Text,
                            capacity: Some(
                                Limit(
                                    45
                                )
                            ),
                            constraints: vec![
                                NotNull,
                                DefaultValue(
                                    Null
                                )
                            ]
                        },
                        stat: None
                    },
                    Column {
                        table: TableName {
                            name: "actor".into(),
                            schema: None,
                            alias: None
                        },
                        name: ColumnName {
                            name: "last_update".into(),
                            table: None,
                            alias: None
                        },
                        comment: None,
                        specification: ColumnSpecification {
                            sql_type: Timestamp,
                            capacity: None,
                            constraints: vec![
                                NotNull,
                                DefaultValue(
                                    Literal::CurrentTimestamp
                                )
                            ]
                        },
                        stat: None
                    }
                ],
                is_view: false,
                table_key: vec![
                    TableKey::PrimaryKey(
                        Key {
                            name: None,
                            columns: vec![
                                ColumnName {
                                    name: "actor_id".into(),
                                    table: None,
                                    alias: None
                                }
                            ]
                        }
                    )
                ]
            }
        );
    }

    #[test]
    fn test_get_table3(){
        let db_url = "sqlite://sakila.db";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let table = "film_actor";
        let table_name = TableName::from(table);
        let table = em.get_table(&table_name);
        assert!(table.is_ok());
        let table = table.unwrap();
        info!("table: {:#?}", table);
        assert_eq!(table,
                    Table {
                        name: TableName {
                            name: "film_actor".into(),
                            schema: None,
                            alias: None
                        },
                        comment: None,
                        columns: vec![
                            Column {
                                table: TableName {
                                    name: "film_actor".into(),
                                    schema: None,
                                    alias: None
                                },
                                name: ColumnName {
                                    name: "actor_id".into(),
                                    table: None,
                                    alias: None
                                },
                                comment: None,
                                specification: ColumnSpecification {
                                    sql_type: Int,
                                    capacity: None,
                                    constraints: vec![
                                        NotNull,
                                        DefaultValue(
                                            Null
                                        )
                                    ]
                                },
                                stat: None
                            },
                            Column {
                                table: TableName {
                                    name: "film_actor".into(),
                                    schema: None,
                                    alias: None
                                },
                                name: ColumnName {
                                    name: "film_id".into(),
                                    table: None,
                                    alias: None
                                },
                                comment: None,
                                specification: ColumnSpecification {
                                    sql_type: Int,
                                    capacity: None,
                                    constraints: vec![
                                        NotNull,
                                        DefaultValue(
                                            Null
                                        )
                                    ]
                                },
                                stat: None
                            },
                            Column {
                                table: TableName {
                                    name: "film_actor".into(),
                                    schema: None,
                                    alias: None
                                },
                                name: ColumnName {
                                    name: "last_update".into(),
                                    table: None,
                                    alias: None
                                },
                                comment: None,
                                specification: ColumnSpecification {
                                    sql_type: Timestamp,
                                    capacity: None,
                                    constraints: vec![
                                        NotNull,
                                        DefaultValue(
                                            Null
                                        )
                                    ]
                                },
                                stat: None
                            }
                        ],
                        is_view: false,
                        table_key: vec![
                            TableKey::PrimaryKey(
                                Key {
                                    name: None,
                                    columns: vec![
                                        ColumnName {
                                            name: "actor_id".into(),
                                            table: None,
                                            alias: None
                                        },
                                        ColumnName {
                                            name: "film_id".into(),
                                            table: None,
                                            alias: None
                                        }
                                    ]
                                }
                            ),
                            TableKey::ForeignKey(
                                ForeignKey {
                                    name: None,
                                    columns: vec![
                                        ColumnName {
                                            name: "film_id".into(),
                                            table: None,
                                            alias: None
                                        }
                                    ],
                                    foreign_table: TableName {
                                        name: "film".into(),
                                        schema: None,
                                        alias: None
                                    },
                                    referred_columns: vec![
                                        ColumnName {
                                            name: "film_id".into(),
                                            table: None,
                                            alias: None
                                        }
                                    ]
                                }
                            ),
                            TableKey::ForeignKey(
                                ForeignKey {
                                    name: None,
                                    columns: vec![
                                        ColumnName {
                                            name: "actor_id".into(),
                                            table: None,
                                            alias: None
                                        }
                                    ],
                                    foreign_table: TableName {
                                        name: "actor".into(),
                                        schema: None,
                                        alias: None
                                    },
                                    referred_columns: vec![
                                        ColumnName {
                                            name: "actor_id".into(),
                                            table: None,
                                            alias: None
                                        }
                                    ]
                                }
                            )
                        ]
                    }
                   );
    }

    #[test]
    fn test_get_foreign(){
        let db_url = "sqlite://sakila.db";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let film = "film_actor";
        let film_table = TableName::from(film);
        let foreign_keys = get_foreign_keys(&em, &film_table);
        assert!(foreign_keys.is_ok());
        assert_eq!(foreign_keys.unwrap(),
                   vec![
                    ForeignKey {
                            name: None,
                            columns: vec![
                                ColumnName::from("film_id"),
                            ],
                            foreign_table: TableName::from("film"),
                            referred_columns: vec![
                                ColumnName::from("film_id")
                            ]
                        },
                        ForeignKey {
                            name: None,
                            columns: vec![
                                ColumnName::from("actor_id"),
                            ],
                            foreign_table: TableName::from("actor"),
                            referred_columns: vec![
                                ColumnName::from("actor_id")
                            ]
                        }
                    ]
                );
    }

    #[test]
    fn test_get_foreign2(){
        let db_url = "sqlite://sakila.db";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let film = "film";
        let film_table = TableName::from(film);
        let foreign_keys = get_foreign_keys(&em, &film_table);
        assert!(foreign_keys.is_ok());
        assert_eq!(foreign_keys.unwrap(),
                   vec![
                    ForeignKey {
                            name: None,
                            columns: vec![
                                ColumnName::from("original_language_id"),
                            ],
                            foreign_table: TableName::from("language"),
                            referred_columns: vec![
                                ColumnName::from("language_id"),
                            ]
                        },
                    ForeignKey {
                            name: None,
                            columns: vec![
                                ColumnName::from("language_id"),
                            ],
                            foreign_table: TableName::from("language"),
                            referred_columns: vec![
                                ColumnName::from("language_id"),
                            ]
                        },
                    ]
                );
    }
}
