//! module table_info extract the table meta data using SQL queries on pg_catalog.
//! This is not using information_schema since there is a performance issue with it.
use r2d2;
use r2d2_postgres;
use r2d2_postgres::TlsMode;
use database::Database;
use dao::{Value};
use error::DbError;
use dao::Rows;
use dao;
use postgres;
use postgres::types::{self,ToSql,FromSql,Type};
use error::PlatformError;
use postgres::types::IsNull;
use std::error::Error;
use std::fmt;
use bigdecimal::BigDecimal;
use dao::TableName;
use dao::ColumnName;
use dao::FromDao;
use entity::EntityManager;
use column::{Column, ColumnConstraint, Literal, ColumnSpecification, Capacity};
use table::Table;
use types::SqlType;
use uuid::Uuid;


/// column name only
#[derive(Debug, FromDao)]
struct ColumnNameSimple{
    column: String,
}


#[derive(Debug, FromDao)]
struct TableKeySimple{
    key_name: String,
    is_primary_key: bool,
    is_unique_key: bool,
    is_foreign_key: bool,
}

#[derive(Debug, FromDao)]
struct ForeignKeySimple{
    key_name: String,
    foreign_table: Option<String>,
    foreign_schema: Option<String>,
}

#[derive(Debug, FromDao)]
struct TableSimple{
    name: String,
    schema: String,
    comment: Option<String>,
    is_view: bool,
}




/// get all the columns of the table
fn get_columns(em: &EntityManager, table_name: &TableName) -> Result<Vec<Column>, DbError> {

    /// column name and comment
    #[derive(Debug, FromDao)]
    struct ColumnSimple{
        number: i32,
        name: String,
        data_type: String,
        comment: Option<String>,
    }

    impl ColumnSimple{
        fn to_column(&self, constraints: Vec<ColumnConstraint>) -> Column {
            let (sql_type, capacity) = self.get_sql_type_capacity();
            println!("sql type: {:?} capacity: {:?}", sql_type, capacity);
            Column{
                table: None,
                name: ColumnName::from(&self.name),
                comment: self.comment.to_owned(),
                specification: ColumnSpecification{
                    sql_type: sql_type, 
                    capacity: capacity,
                    constraints: constraints,
                }
            }
        }

        fn get_sql_type_capacity(&self) -> (SqlType, Option<Capacity>) {
            let data_type: &str = &self.data_type;
            println!("data_type: {}", data_type);
            let start = data_type.find('(');
            let end = data_type.find(')');
            let (dtype, capacity) = if let Some(start) = start {
                if let Some(end) = end {
                    let dtype = &data_type[0..start];
                    let range = &data_type[start+1..end];
                    let capacity = if range.contains(","){
                        let splinters = range.split(",").collect::<Vec<&str>>();
                        assert!(splinters.len() == 2, "There should only be 2 parts");
                        let r1:i32 = splinters[0].parse().unwrap();
                        let r2:i32= splinters[1].parse().unwrap();
                        Capacity::Range(r1,r2)
                    }
                    else{
                        let limit:i32 = range.parse().unwrap();
                        Capacity::Limit(limit)
                    };
                    println!("data_type: {}", dtype);
                    println!("range: {}", range);
                    (dtype, Some(capacity))
                }else{
                    (data_type, None)
                }
            }
            else{
                (data_type, None)
            };

            let sql_type = match dtype{
                "boolean" => SqlType::Bool,
                "tinyint" => SqlType::Tinyint,
                "smallint" | "year" => SqlType::Smallint,
                "int" | "integer" => SqlType::Int,
                "bigint" => SqlType::Bigint,
                "smallserial" => SqlType::SmallSerial,
                "serial" => SqlType::Serial,
                "bigserial" => SqlType::BigSerial,
                "real" => SqlType::Real,
                "float" => SqlType::Float,
                "double" => SqlType::Double,
                "numeric" => SqlType::Numeric,
                "tinyblob" => SqlType::Tinyblob,
                "mediumblob" => SqlType::Mediumblob,
                "blob" => SqlType::Blob,
                "longblob" => SqlType::Longblob,
                "varbinary" => SqlType::Varbinary,
                "char" => SqlType::Char,
                "varchar" | "character varying" => SqlType::Varchar,
                "tinytext" => SqlType::Tinytext,
                "mediumtext" => SqlType::Mediumtext,
                "text" => SqlType::Text,
                "text[]" => SqlType::TextArray,
                "uuid" => SqlType::Uuid,
                "date" => SqlType::Date,
                "timestamp" | "timestamp without time zone" => SqlType::Timestamp,
                "timestamp with time zone" => SqlType::TimestampTz,
                _ => SqlType::Custom(data_type.to_owned()), 
            };
            (sql_type, capacity)
        }
    }
    let sql = "SELECT \
                 pg_attribute.attnum AS number, \
                 pg_attribute.attname AS name, \
                 pg_catalog.format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS data_type, \
                 pg_description.description AS comment \
            FROM pg_attribute \
       LEFT JOIN pg_class \
              ON pg_class.oid = pg_attribute.attrelid \
       LEFT JOIN pg_namespace \
              ON pg_namespace.oid = pg_class.relnamespace \
       LEFT JOIN pg_description \
              ON pg_description.objoid = pg_class.oid \
             AND pg_description.objsubid = pg_attribute.attnum \
           WHERE
                 pg_class.relname = $1 \
             AND pg_namespace.nspname = $2 \
             AND pg_attribute.attnum > 0 \
             AND pg_attribute.attisdropped = false \
        ORDER BY number\
    ";
    let schema = match table_name.schema {
        Some(ref schema) => schema.to_string(),
        None => "public".to_string()
    };
    println!("sql: {}", sql);
    let columns_simple: Result<Vec<ColumnSimple>, DbError> = 
        em.execute_sql_with_return(&sql, &[&table_name.name, &schema]);

    match columns_simple{
        Ok(columns_simple) => {
            let mut columns = vec![];
            for column_simple in columns_simple{
                let (sql_type,_) = column_simple.get_sql_type_capacity();
                let constraint = get_column_constraint(em, table_name, &column_simple.name,
                                                       sql_type);
                match constraint{
                    Ok(constraint) => {
                        let column = column_simple.to_column(constraint);
                        columns.push(column);
                    },
                    Err(e) => {return Err(e);},
                }
            }
            Ok(columns)
        },
        Err(e) => Err(e),
    }
}


/// get the contrainst of each of this column
fn get_column_constraint(em: &EntityManager, table_name: &TableName, column_name: &String, sql_type: SqlType)
    -> Result<Vec<ColumnConstraint>, DbError> {

    /// null, datatype default value
    #[derive(Debug, FromDao)]
    struct ColumnConstraintSimple{
        not_null: bool,
        default: Option<String>,
    }

    impl ColumnConstraintSimple{

        fn to_column_constraints(&self, sql_type: SqlType) -> Vec<ColumnConstraint> {
            let mut constraints = vec![];
            if self.not_null{
                constraints.push(ColumnConstraint::NotNull);
            }
            if let Some(ref default) = self.default{
                let constraint = if default == "null" {
                    ColumnConstraint::DefaultValue(Literal::Null)
                }
                else if default.starts_with("nextval"){
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
                            | SqlType::Numeric => {
                                let v: Result<f64,_> = default.parse();
                                match v{
                                    Ok(v) => Literal::Double(v),
                                    Err(e) => panic!("error parsing to f64: {} error: {}", default,
                                                    e)
                                }

                            }
                        SqlType::Uuid => {
                            if default == "uuid_generate_v4()"{
                               Literal::UuidGenerateV4
                            }
                            else{
                                let v: Result<Uuid,_> = Uuid::parse_str(default);
                                match v{
                                    Ok(v) => Literal::Uuid(v),
                                    Err(e) => panic!("error parsing to uuid: {} error: {}", default, e)
                                }
                            }
                        }
                        SqlType::Timestamp
                            | SqlType::TimestampTz
                            => {
                                if default == "now()" {
                                    Literal::CurrentTimestamp
                                }
                                else{
                                    panic!("timestamp other than now is not covered")
                                }
                            }
                        SqlType::Date => {
                            if default == "today()" {
                                Literal::CurrentDate
                            }else{
                                panic!("date other than today is not covered")
                            }
                        }
                        SqlType::Varchar 
                            | SqlType::Char
                            | SqlType::Tinytext
                            | SqlType::Mediumtext
                            | SqlType::Text
                                => Literal::String(default.to_owned()),
                        SqlType::Custom(s) => Literal::String(default.to_owned()),
                        _ => panic!("not convered: {:?}", sql_type),
                    };
                    ColumnConstraint::DefaultValue(literal)
                };
                constraints.push(constraint);
                
            }
            constraints
        }

    }

    let sql = "SELECT \
               pg_attribute.attnotnull AS not_null, \
     CASE WHEN pg_attribute.atthasdef THEN pg_attrdef.adsrc \
           END AS default \
          FROM pg_attribute \
          JOIN pg_class \
            ON pg_class.oid = pg_attribute.attrelid \
          JOIN pg_type \
            ON pg_type.oid = pg_attribute.atttypid \
     LEFT JOIN pg_attrdef \
            ON pg_attrdef.adrelid = pg_class.oid \
           AND pg_attrdef.adnum = pg_attribute.attnum \
     LEFT JOIN pg_namespace \
            ON pg_namespace.oid = pg_class.relnamespace \
     LEFT JOIN pg_constraint \
            ON pg_constraint.conrelid = pg_class.oid \
           AND pg_attribute.attnum = ANY (pg_constraint.conkey) \
         WHERE 
               pg_attribute.attname = $1 \
           AND pg_class.relname = $2 \
           AND pg_namespace.nspname = $3 \
           AND pg_attribute.attisdropped = false\
    ";
    let schema = match table_name.schema {
        Some(ref schema) => schema.to_string(),
        None => "public".to_string()
    };
    let column_constraint: Result<ColumnConstraintSimple, DbError> = 
        em.execute_sql_with_one_return(&sql, &[&column_name, &table_name.name, &schema]);
    column_constraint
        .map(|c| c.to_column_constraints(sql_type) )
}

/// get the column names involved in a Primary key or unique key
fn get_column_name_from_key(em: &EntityManager, key_name: &String, table_name: &TableName) 
    -> Result<Vec<ColumnNameSimple>, DbError> {
    let sql = "SELECT pg_attribute.attname as column \
        FROM pg_attribute \
        JOIN pg_class \
          ON pg_class.oid = pg_attribute.attrelid \
   LEFT JOIN pg_namespace \
          ON pg_namespace.oid = pg_class.relnamespace \
   LEFT JOIN pg_constraint \
          ON pg_constraint.conrelid = pg_class.oid \
         AND pg_attribute.attnum = ANY (pg_constraint.conkey) \
       WHERE pg_namespace.nspname = $3 \
         AND pg_class.relname = $2 \
         AND pg_attribute.attnum > 0 \
         AND pg_constraint.conname = $1 \
        ";
    let schema = match table_name.schema {
        Some(ref schema) => schema.to_string(),
        None => "public".to_string()
    };

    let column_name_simple: Result<Vec<ColumnNameSimple>, DbError> = 
        em.execute_sql_with_return(&sql, &[&key_name, &table_name.name, &schema]);
    column_name_simple
}


/// get the Primary keys, Unique keys of this table
fn get_table_key(em: &EntityManager, table_name: &TableName) -> Result<Vec<TableKeySimple>, DbError> {
    let sql = "SELECT conname AS key_name, \
        CASE WHEN contype = 'p' THEN true ELSE false END AS is_primary_key, \
        CASE WHEN contype = 'u' THEN true ELSE false END AS is_unique_key, \
        CASE WHEN contype = 'f' THEN true ELSE false END AS is_foreign_key \
        FROM pg_constraint \
   LEFT JOIN pg_class  \
          ON pg_class.oid = pg_constraint.conrelid \
   LEFT JOIN pg_namespace \
          ON pg_namespace.oid = pg_class.relnamespace \
   LEFT JOIN pg_class AS g
          ON pg_constraint.confrelid = g.oid
       WHERE pg_class.relname = $1 \
         AND pg_namespace.nspname = $2 \
    ";

    let schema = match table_name.schema {
        Some(ref schema) => schema.to_string(),
        None => "public".to_string()
    };

    let table_keys: Result<Vec<TableKeySimple>, DbError> = 
        em.execute_sql_with_return(&sql, &[&table_name.name, &schema]);
    table_keys
}

/// get the foreign key detail of this key name 
fn get_foreign_key(em: &EntityManager, foreign_key: &String) -> Result<Vec<ForeignKeySimple>, DbError> {
    let sql = "SELECT conname AS key_name, \
        pg_class.relname AS foreign_table, \
        (SELECT pg_namespace.nspname FROM pg_namespace WHERE pg_namespace.oid = pg_class.relnamespace) AS foreign_schema \
        FROM pg_constraint \
   LEFT JOIN pg_class \
          ON pg_constraint.confrelid = pg_class.oid \
       WHERE pg_constraint.conname = $1
    ";

    let foreign_keys: Result<Vec<ForeignKeySimple>, DbError> = 
        em.execute_sql_with_return(&sql, &[&foreign_key]);
    foreign_keys
}

fn get_referred_foreign_columns(em: &EntityManager, foreign_key: &String) -> Result<Vec<ColumnNameSimple>, DbError> {
    let sql = "SELECT conname AS key_name, \
        pg_attribute.attname AS column \
        FROM pg_constraint \
   LEFT JOIN pg_class \
          ON pg_constraint.confrelid = pg_class.oid \
   LEFT JOIN pg_attribute \
          ON pg_attribute.attnum = ANY (pg_constraint.confkey) \
         AND pg_class.oid = pg_attribute.attrelid \
       WHERE pg_constraint.conname = $1
    ";

    let foreign_columns: Result<Vec<ColumnNameSimple>, DbError> = 
        em.execute_sql_with_return(&sql, &[&foreign_key]);
    foreign_columns
}

/// get the Primary keys, Unique keys of this table
fn get_table_simple(em: &EntityManager, table_name: &TableName) -> Result<Vec<TableSimple>, DbError> {

    let sql = "SELECT pg_class.relname as name, \
                pg_namespace.nspname as schema, \
   CASE WHEN pg_class.relkind = 'v' THEN true ELSE false \
         END AS is_view, 
                obj_description(pg_class.oid) as comment \
        FROM pg_class \
   LEFT JOIN pg_namespace \
          ON pg_namespace.oid = pg_class.relnamespace \
       WHERE pg_class.relname = $1 \
         AND pg_namespace.nspname = $2 \
    ";

    let schema = match table_name.schema {
        Some(ref schema) => schema.to_string(),
        None => "public".to_string()
    };

    let table_simple: Result<Vec<TableSimple>, DbError> = 
        em.execute_sql_with_return(&sql, &[&table_name.name, &schema]);
    table_simple
}


#[cfg(test)]
mod test{

    use super::*;
    use pool::Pool;


    #[test]
    fn column_constraint_for_actor_id(){
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let actor_table = TableName::from("actor");
        let actor_id_column = ColumnName::from("actor_id");
        let column = get_column_constraint(&em, &actor_table, &actor_id_column.name, SqlType::Int);
        println!("column: {:#?}", column);
        assert!(column.is_ok());
        let constraints = column.unwrap();
        println!("constraints: {:#?}", constraints);
        assert_eq!(constraints.len(), 2);
        assert_eq!(constraints, vec![ColumnConstraint::NotNull, ColumnConstraint::AutoIncrement]);
    }
    #[test]
    fn column_constraint_for_actor_last_updated(){
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let actor_table = TableName::from("actor");
        let actor_id_column = ColumnName::from("last_update");
        let column = get_column_constraint(&em, &actor_table, &actor_id_column.name,
                                           SqlType::Timestamp);
        println!("column: {:#?}", column);
        assert!(column.is_ok());
        let constraints = column.unwrap();
        println!("constraints: {:#?}", constraints);
        assert_eq!(constraints.len(), 2);
        assert_eq!(constraints, vec![ColumnConstraint::NotNull,
                   ColumnConstraint::DefaultValue(Literal::CurrentTimestamp)]);
    }

    #[test]
    fn column_for_actor(){
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let actor_table = TableName::from("actor");
        let columns = get_columns(&em, &actor_table);
        println!("columns: {:#?}", columns);
        assert!(columns.is_ok());
        let columns = columns.unwrap();
        assert_eq!(columns.len(), 4);
        assert_eq!(columns[1], 
                   Column{
                       table: None,
                       name: ColumnName::from("first_name"),
                       comment: None,
                       specification: ColumnSpecification{
                           sql_type: SqlType::Varchar,
                           capacity: Some(Capacity::Limit(45)),
                           constraints: vec![ColumnConstraint::NotNull],
                       }
                    });
    }

    #[test]
    fn column_for_film(){
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let table = TableName::from("film");
        let columns = get_columns(&em, &table);
        println!("columns: {:#?}", columns);
        assert!(columns.is_ok());
        let columns = columns.unwrap();
        assert_eq!(columns.len(), 14);
        assert_eq!(columns[7], 
                   Column{
                       table: None,
                       name: ColumnName::from("rental_rate"),
                       comment: None,
                       specification: ColumnSpecification{
                           sql_type: SqlType::Numeric,
                           capacity: Some(Capacity::Range(4,2)),
                           constraints: vec![ColumnConstraint::NotNull,
                                    ColumnConstraint::DefaultValue(Literal::Double(4.99))
                                ],
                       }
                    });
    }
}
