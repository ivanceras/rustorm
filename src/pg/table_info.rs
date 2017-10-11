
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
use column::Column;
use table::Table;

/// column name and comment
#[derive(Debug, FromDao)]
struct ColumnSimple{
    number: i32,
    name: String,
    comment: Option<String>,
}

/// column name only
#[derive(Debug, FromDao)]
struct ColumnNameSimple{
    column: String,
}

/// null, datatype default value
#[derive(Debug, FromDao)]
struct ColumnConstraint{
    not_null: bool,
    data_type: String,
    default: Option<String>,
}

#[derive(Debug, FromDao)]
struct TableKeySimple{
    key_name: String,
    primary: bool,
    unique: bool,
    foreign: bool,
}




/// get the Table meta
fn get_table(em: &EntityManager, table_name: &TableName ) -> Result<Table, DbError> {
    let columns: Result<Vec<ColumnSimple>, DbError> = 
            get_columns(&em, &table_name);
    if let Ok(columns) = columns{
        for col in columns{
            let column_name = ColumnName{
                name: col.name,
                table: Some(table_name.name.to_owned()),
                alias: None,
            };
            let column_constraint: Result<Vec<ColumnConstraint>, DbError> =
                    get_column_constraint(em, table_name, &column_name);
        }
    }

    let table_keys: Result<Vec<TableKeySimple>, DbError> = 
            get_table_key(&em, &table_name);
    if let Ok(table_keys) = table_keys{
        for table_key in table_keys{
            let key_member: Result<Vec<ColumnNameSimple>, DbError> =
                    get_column_name_from_key(&em, &table_key.key_name, &table_name);
        }
    }
    Ok(Table{
        name: table_name.to_owned(),
        parent_table: None,
        sub_table: vec![],
        comment: None,
        columns: vec![],
        is_view: false,
        table_key: vec![],
    })
}


/// get all the columns of the table
fn get_columns(em: &EntityManager, table_name: &TableName) -> Result<Vec<ColumnSimple>, DbError> {
    let sql = "SELECT\
                 pg_attribute.attnum AS number,\
                 pg_attribute.attname AS name,\
                 pg_description.description AS comment\
            FROM pg_attribute\
            JOIN pg_class\
              ON pg_class.oid = pg_attribute.attrelid\
       LEFT JOIN pg_namespace\
              ON pg_namespace.oid = pg_class.relnamespace\
       LEFT JOIN pg_description\
              ON pg_description.objoid = pg_class.oid\
             AND pg_description.objsubid = pg_attribute.attnum\
           WHERE pg_class.relkind IN ('r','v')\
             AND pg_class.relname = $1\
             AND pg_namespace.nspname = $2\
             AND pg_attribute.attnum > 0\
             AND pg_attribute.attisdropped = false\
        ORDER BY number\
    ";
    let schema = match table_name.schema {
        Some(ref schema) => schema.to_string(),
        None => "public".to_string()
    };
    let columns: Result<Vec<ColumnSimple>, DbError> = 
        em.execute_sql_with_return(&sql, &[&table_name.name, &schema]);

    columns
}


/// get the contrainst of each of this column
fn get_column_constraint(em: &EntityManager, table_name: &TableName, column_name: &ColumnName)
    -> Result<Vec<ColumnConstraint>, DbError> {
    let sql = "SELECT\
               pg_attribute.attnotnull AS not_null,\
               pg_catalog.format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS data_type,\
     CASE WHEN pg_attribute.atthasdef THEN pg_attrdef.adsrc\
           END as default\
          FROM pg_attribute\
          JOIN pg_class\
            ON pg_class.oid = pg_attribute.attrelid\
          JOIN pg_type\
            ON pg_type.oid = pg_attribute.atttypid\
     LEFT JOIN pg_attrdef\
            ON pg_attrdef.adrelid = pg_class.oid\
           AND pg_attrdef.adnum = pg_attribute.attnum\
     LEFT JOIN pg_namespace\
            ON pg_namespace.oid = pg_class.relnamespace\
     LEFT JOIN pg_constraint\
            ON pg_constraint.conrelid = pg_class.oid\
           AND pg_attribute.attnum = ANY (pg_constraint.conkey)\
         WHERE pg_class.relkind IN ('r','v')\
           AND pg_attribute.attname = $1\
           AND pg_class.relname = $2\
           AND pg_namespace.nspname = $3\
           AND pg_attribute.attisdropped = false\
    ";
    let schema = match table_name.schema {
        Some(ref schema) => schema.to_string(),
        None => "public".to_string()
    };
    let column_constraint: Result<Vec<ColumnConstraint>, DbError> = 
        em.execute_sql_with_return(&sql, &[&column_name.name, &table_name.name, &schema]);
    column_constraint
}

/// get the column names involved in a Primary key or unique key
fn get_column_name_from_key(em: &EntityManager, key_name: &String, table_name: &TableName) 
    -> Result<Vec<ColumnNameSimple>, DbError> {
    let sql = "SELECT attname as column\
        FROM pg_attribute\
        JOIN pg_class\
          ON pg_class.oid = pg_attribute.attrelid\
   LEFT JOIN pg_namespace\
          ON pg_namespace.oid = pg_class.relnamespace\
   LEFT JOIN pg_constraint\
          ON pg_constraint.conrelid = pg_class.oid\
         AND pg_attribute.attnum = ANY (pg_constraint.conkey)\
       WHERE pg_namespace.nspname = $3\
         AND pg_class.relname = $2\
         AND pg_attribute.attnum > 0\
         AND pg_constraint.conname = $1\
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
    let sql = "SELECT conname AS key_name,\
        CASE WHEN contype = 'p' THEN true ELSE false END AS primary,\
        CASE WHEN contype = 'u' THEN true ELSE false END AS unique,\
        CASE WHEN contype = 'f' THEN true ELSE false END AS foreign\
        FROM pg_constraint\
   LEFT JOIN pg_class \
          ON pg_class.oid = pg_constraint.conrelid\
   LEFT JOIN pg_namespace\
          ON pg_namespace.oid = pg_class.relnamespace\
       WHERE pg_class.relname = $1\
         AND pg_namespace.nspname = $2\
    ";

    let schema = match table_name.schema {
        Some(ref schema) => schema.to_string(),
        None => "public".to_string()
    };

    let table_keys: Result<Vec<TableKeySimple>, DbError> = 
        em.execute_sql_with_return(&sql, &[&table_name.name, &schema]);
    table_keys
}


#[cfg(test)]
mod test{

    use super::*;
    use pool::Pool;

    #[test]
    fn extract_table_info(){
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let table_name = TableName::from("film_actor");
        let table = get_table(&em, &table_name);
        assert!(table.is_ok());
    }
}
