use crate::{
    column,
    common,
    table::SchemaContent,
    types::SqlType,
    users::{
        Role,
        User,
    },
    Column,
    ColumnName,
    DataError,
    Database,
    DatabaseName,
    DbError,
    FromDao,
    Table,
    TableName,
    Value,
};
use r2d2::ManageConnection;
use r2d2_mysql::{
    self,
    mysql,
};
use rustorm_dao::{
    FromDao,
    Rows,
};
use thiserror::Error;

pub fn init_pool(
    db_url: &str,
) -> Result<r2d2::Pool<r2d2_mysql::MysqlConnectionManager>, MysqlError> {
    test_connection(db_url)?;
    let opts = mysql::Opts::from_url(&db_url)?;
    let builder = mysql::OptsBuilder::from_opts(opts);
    let manager = r2d2_mysql::MysqlConnectionManager::new(builder);
    let pool = r2d2::Pool::new(manager)?;
    Ok(pool)
}

pub fn test_connection(db_url: &str) -> Result<(), MysqlError> {
    let opts = mysql::Opts::from_url(&db_url)?;
    let builder = mysql::OptsBuilder::from_opts(opts);
    let manager = r2d2_mysql::MysqlConnectionManager::new(builder);
    let mut conn = manager.connect()?;
    manager.is_valid(&mut conn)?;
    Ok(())
}

pub struct MysqlDB(pub r2d2::PooledConnection<r2d2_mysql::MysqlConnectionManager>);

impl Database for MysqlDB {
    fn execute_sql_with_return(&mut self, sql: &str, param: &[&Value]) -> Result<Rows, DbError> {
        fn collect(mut rows: mysql::QueryResult) -> Result<Rows, DbError> {
            let column_types: Vec<_> = rows.columns_ref().iter().map(|c| c.column_type()).collect();

            let column_names = rows
                .columns_ref()
                .iter()
                .map(|c| std::str::from_utf8(c.name_ref()).map(ToString::to_string))
                .collect::<Result<Vec<String>, _>>()
                .map_err(|e| MysqlError::Utf8Error(e))?;

            let mut records = Rows::new(column_names);
            while rows.more_results_exists() {
                for r in rows.by_ref() {
                    records.push(into_record(r.map_err(MysqlError::from)?, &column_types)?);
                }
            }

            Ok(records)
        }

        if param.is_empty() {
            let rows = self
                .0
                .query(&sql)
                .map_err(|e| MysqlError::SqlError(e, sql.to_string()))?;

            collect(rows)
        } else {
            let mut stmt = self
                .0
                .prepare(&sql)
                .map_err(|e| MysqlError::SqlError(e, sql.to_string()))?;

            let params: mysql::Params = param
                .iter()
                .map(|v| MyValue(v))
                .map(|v| mysql::prelude::ToValue::to_value(&v))
                .collect::<Vec<_>>()
                .into();

            let rows = stmt
                .execute(&params)
                .map_err(|e| MysqlError::SqlError(e, sql.to_string()))?;

            collect(rows)
        }
    }

    fn get_table(&mut self, table_name: &TableName) -> Result<Table, DbError> {
        #[derive(Debug, FromDao)]
        struct TableSpec {
            schema: String,
            name: String,
            comment: String,
            is_view: i32,
        }

        let schema = table_name
            .schema
            .as_ref()
            .map(String::as_str)
            .unwrap_or("__DUMMY__")
            .into();
        let table_name = &table_name.name.clone().into();

        let mut tables: Vec<TableSpec> = self
            .execute_sql_with_return(
                r#"
                SELECT TABLE_SCHEMA AS `schema`,
                       TABLE_NAME AS name,
                       TABLE_COMMENT AS comment,
                       CASE TABLE_TYPE WHEN 'VIEW' THEN TRUE ELSE FALSE END AS is_view
                  FROM INFORMATION_SCHEMA.TABLES
                 WHERE TABLE_SCHEMA = CASE ? WHEN '__DUMMY__' THEN DATABASE() ELSE ? END AND TABLE_NAME = ?"#,
                &[
                    &schema, &schema,
                    &table_name,
                ],
            )?
            .iter()
            .map(|dao| FromDao::from_dao(&dao))
            .collect();

        let table_spec = match tables.len() {
            0 => return Err(DbError::DataError(DataError::ZeroRecordReturned)),
            _ => tables.remove(0),
        };

        #[derive(Debug, FromDao)]
        struct ColumnSpec {
            schema: String,
            table_name: String,
            name: String,
            comment: String,
            type_: String,
        }

        let columns: Vec<Column> = self
            .execute_sql_with_return(
                r#"
                SELECT TABLE_SCHEMA AS `schema`,
                       TABLE_NAME AS table_name,
                       COLUMN_NAME AS name,
                       COLUMN_COMMENT AS comment,
                       CAST(COLUMN_TYPE as CHAR(64)) AS type_
                  FROM INFORMATION_SCHEMA.COLUMNS
                 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"#,
                &[&table_spec.schema.clone().into(), &table_name],
            )?
            .iter()
            .map(|dao| FromDao::from_dao(&dao))
            .map(|spec: ColumnSpec| {
                let (sql_type, capacity) =
                    if spec.type_.starts_with("enum(") || spec.type_.starts_with("set(") {
                        let start = spec.type_.find('(');
                        let end = spec.type_.find(')');
                        if let (Some(start), Some(end)) = (start, end) {
                            let dtype = &spec.type_[0..start];
                            let range = &spec.type_[start + 1..end];
                            let choices = range
                                .split(',')
                                .map(|v| v.to_owned())
                                .collect::<Vec<String>>();

                            match dtype {
                                "enum" => (SqlType::Enum(dtype.to_owned(), choices), None),
                                "set" => (SqlType::Enum(dtype.to_owned(), choices), None),
                                _ => panic!("not yet handled: {}", dtype),
                            }
                        } else {
                            panic!("not yet handled: {}", spec.type_)
                        }
                    } else {
                        let (dtype, capacity) = common::extract_datatype_with_capacity(&spec.type_);
                        let sql_type = match &*dtype {
                            "tinyint" => SqlType::Tinyint,
                            "smallint" | "year" => SqlType::Smallint,
                            "mediumint" => SqlType::Int,
                            "int" => SqlType::Int,
                            "bigint" => SqlType::Bigint,
                            "float" => SqlType::Float,
                            "double" => SqlType::Double,
                            "decimal" => SqlType::Numeric,
                            "tinyblob" => SqlType::Tinyblob,
                            "mediumblob" => SqlType::Mediumblob,
                            "blob" => SqlType::Blob,
                            "longblob" => SqlType::Longblob,
                            "binary" | "varbinary" => SqlType::Varbinary,
                            "char" => SqlType::Char,
                            "varchar" => SqlType::Varchar,
                            "tinytext" => SqlType::Tinytext,
                            "mediumtext" => SqlType::Mediumtext,
                            "text" | "longtext" => SqlType::Text,
                            "date" => SqlType::Date,
                            "datetime" | "timestamp" => SqlType::Timestamp,
                            "time" => SqlType::Time,
                            _ => panic!("not yet handled: {}", dtype),
                        };

                        (sql_type, capacity)
                    };

                Column {
                    table: TableName::from(&format!("{}.{}", spec.schema, spec.table_name)),
                    name: ColumnName::from(&spec.name),
                    comment: Some(spec.comment),
                    specification: column::ColumnSpecification {
                        capacity,
                        // TODO: implementation
                        constraints: vec![],
                        sql_type,
                    },
                    stat: None,
                }
            })
            .collect();

        Ok(Table {
            name: TableName {
                name: table_spec.name,
                schema: Some(table_spec.schema),
                alias: None,
            },
            comment: Some(table_spec.comment),
            columns,
            is_view: table_spec.is_view == 1,
            // TODO: implementation
            table_key: vec![],
        })
    }

    fn get_all_tables(&mut self) -> Result<Vec<Table>, DbError> { todo!() }

    fn get_grouped_tables(&mut self) -> Result<Vec<SchemaContent>, DbError> { todo!() }

    fn get_users(&mut self) -> Result<Vec<User>, DbError> { todo!() }

    fn get_roles(&mut self, _username: &str) -> Result<Vec<Role>, DbError> { todo!() }

    fn get_database_name(&mut self) -> Result<Option<DatabaseName>, DbError> { todo!() }
}

#[derive(Debug)]
pub struct MyValue<'a>(&'a Value);

impl mysql::prelude::ToValue for MyValue<'_> {
    fn to_value(&self) -> mysql::Value {
        match self.0 {
            Value::Bool(ref v) => v.into(),
            Value::Tinyint(ref v) => v.into(),
            Value::Smallint(ref v) => v.into(),
            Value::Int(ref v) => v.into(),
            Value::Bigint(ref v) => v.into(),
            Value::Float(ref v) => v.into(),
            Value::Double(ref v) => v.into(),
            Value::Blob(ref v) => v.into(),
            Value::ImageUri(ref _v) => {
                panic!("ImageUri is only used for reading data from DB, not inserting into DB")
            }
            Value::Char(ref v) => v.to_string().into(),
            Value::Text(ref v) => v.into(),
            Value::Uuid(ref v) => v.as_bytes().into(),
            Value::Date(ref v) => v.into(),
            Value::Timestamp(ref v) => v.naive_utc().into(),
            Value::DateTime(ref v) => v.into(),
            Value::Time(ref v) => v.into(),
            Value::Interval(ref _v) => panic!("storing interval in DB is not supported"),
            Value::Json(ref v) => v.into(),
            Value::Nil => mysql::Value::NULL,
            Value::BigDecimal(_) => unimplemented!("we need to upgrade bigdecimal crate"),
            Value::Point(_) | Value::Array(_) => unimplemented!("unsupported type"),
        }
    }
}

fn into_record(
    mut row: mysql::Row,
    column_types: &[mysql::consts::ColumnType],
) -> Result<Vec<Value>, MysqlError> {
    use mysql::{
        consts::ColumnType,
        from_value_opt as fvo,
    };

    column_types
        .iter()
        .enumerate()
        .map(|(i, column_type)| {
            let cell: mysql::Value = row
                .take_opt(i)
                .unwrap_or_else(|| unreachable!("column length does not enough"))
                .unwrap_or_else(|_| unreachable!("could not convert as `mysql::Value`"));

            if cell == mysql::Value::NULL {
                return Ok(Value::Nil);
            }

            match column_type {
                ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
                    fvo(cell)
                        .and_then(|v: Vec<u8>| {
                            bigdecimal::BigDecimal::parse_bytes(&v, 10)
                                .ok_or(mysql::FromValueError(mysql::Value::Bytes(v)))
                        })
                        .map(Value::BigDecimal)
                }
                ColumnType::MYSQL_TYPE_TINY => fvo(cell).map(Value::Tinyint),
                ColumnType::MYSQL_TYPE_SHORT | ColumnType::MYSQL_TYPE_YEAR => {
                    fvo(cell).map(Value::Smallint)
                }
                ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_INT24 => {
                    fvo(cell).map(Value::Int)
                }
                ColumnType::MYSQL_TYPE_LONGLONG => fvo(cell).map(Value::Bigint),
                ColumnType::MYSQL_TYPE_FLOAT => fvo(cell).map(Value::Float),
                ColumnType::MYSQL_TYPE_DOUBLE => fvo(cell).map(Value::Double),
                ColumnType::MYSQL_TYPE_NULL => fvo(cell).map(|_: mysql::Value| Value::Nil),
                ColumnType::MYSQL_TYPE_TIMESTAMP => {
                    fvo(cell).map(|v: chrono::NaiveDateTime| {
                        Value::Timestamp(chrono::DateTime::from_utc(v, chrono::Utc))
                    })
                }
                ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => {
                    fvo(cell).map(Value::Date)
                }
                ColumnType::MYSQL_TYPE_TIME => fvo(cell).map(Value::Time),
                ColumnType::MYSQL_TYPE_DATETIME => fvo(cell).map(Value::DateTime),
                ColumnType::MYSQL_TYPE_VARCHAR
                | ColumnType::MYSQL_TYPE_VAR_STRING
                | ColumnType::MYSQL_TYPE_STRING => fvo(cell).map(Value::Text),
                ColumnType::MYSQL_TYPE_JSON => fvo(cell).map(Value::Json),
                ColumnType::MYSQL_TYPE_TINY_BLOB
                | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
                | ColumnType::MYSQL_TYPE_LONG_BLOB
                | ColumnType::MYSQL_TYPE_BLOB => fvo(cell).map(Value::Blob),
                ColumnType::MYSQL_TYPE_TIMESTAMP2
                | ColumnType::MYSQL_TYPE_DATETIME2
                | ColumnType::MYSQL_TYPE_TIME2 => {
                    panic!("only used in server side: {:?}", column_type)
                }
                ColumnType::MYSQL_TYPE_BIT
                | ColumnType::MYSQL_TYPE_ENUM
                | ColumnType::MYSQL_TYPE_SET
                | ColumnType::MYSQL_TYPE_GEOMETRY => {
                    panic!("not yet handling this kind: {:?}", column_type)
                }
            }
            .map_err(MysqlError::from)
        })
        .collect()
}

#[derive(Debug, Error)]
pub enum MysqlError {
    #[error("{1}")]
    GenericError(String, mysql::Error),
    #[error("{0}")]
    UrlError(#[from] mysql::UrlError),
    #[error("Error executing {1}: {0}")]
    SqlError(mysql::Error, String),
    #[error("{0}")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("{0}")]
    ConvertError(#[from] mysql::FromValueError),
    #[error("Pool initialization error: {0}")]
    PoolInitializationError(#[from] r2d2::Error),
}

impl From<mysql::Error> for MysqlError {
    fn from(e: mysql::Error) -> Self { MysqlError::GenericError("From conversion".into(), e) }
}
