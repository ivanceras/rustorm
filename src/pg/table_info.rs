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
use table::{Table,TableKey,PrimaryKey, UniqueKey,ForeignKey};
use types::SqlType;
use uuid::Uuid;
use pg::column_info;








/// get the table definition, its columns and table_keys 
fn get_table(em: &EntityManager, table_name: &TableName) -> Result<Table, DbError> {

    #[derive(Debug, FromDao)]
    struct TableSimple{
        name: String,
        schema: String,
        comment: Option<String>,
        is_view: bool,
    }

    impl TableSimple{

        fn to_table(self, columns: Vec<Column>, table_key: Vec<TableKey>) -> Table {
            Table{
                name: TableName{
                          name: self.name,
                          schema: Some(self.schema),
                          alias: None,
                      },
                comment: self.comment,
                columns: columns,
                is_view: self.is_view,
                table_key: table_key,
            }
        }
    }


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

    let table_simple: Result<TableSimple, DbError> = 
        em.execute_sql_with_one_return(&sql, &[&table_name.name, &schema]);
    println!("table simple: {:#?}", table_simple);

    match table_simple{
        Ok(table_simple) => {
            let columns: Result<Vec<Column>, DbError>
                = column_info::get_columns(em, table_name);
            match columns {
                Ok(columns) => {
                    let keys: Result<Vec<TableKey>,DbError> = get_table_key(em, table_name); 
                    match keys{
                        Ok(keys) => {
                            let table = table_simple.to_table(columns, keys);
                            Ok(table)
                        }
                        Err(e) => Err(e)
                    }
                }
                Err(e) => {return Err(e);}
            }
        }
        Err(e) => Err(e)
    }
}

/// column name only
#[derive(Debug, FromDao)]
struct ColumnNameSimple{
    column: String,
}
impl ColumnNameSimple{
    fn to_columnname(self) -> ColumnName {
        ColumnName{
            name: self.column,
            table: None,
            alias: None,
        }
    }
}

/// get the column names involved in a Primary key or unique key
fn get_columnname_from_key(em: &EntityManager, key_name: &String, table_name: &TableName) 
    -> Result<Vec<ColumnName>, DbError> {


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
    match column_name_simple{
        Ok(column_name_simple) => {
            let mut column_names = vec![];
            for simple in column_name_simple{
                let column_name = simple.to_columnname();
                column_names.push(column_name);
            }
            Ok(column_names)
        }
        Err(e) => Err(e)
    }
}


/// get the Primary keys, Unique keys of this table
fn get_table_key(em: &EntityManager, table_name: &TableName) -> Result<Vec<TableKey>, DbError> {

    #[derive(Debug, FromDao)]
    struct TableKeySimple{
        key_name: String,
        is_primary_key: bool,
        is_unique_key: bool,
        is_foreign_key: bool,
    }

    impl TableKeySimple{
        fn to_table_key(self, em: &EntityManager, table_name: &TableName) -> TableKey {
            if self.is_primary_key{
                let primary = PrimaryKey{
                    name: Some(self.key_name.to_owned()),
                    columns: get_columnname_from_key(em, &self.key_name, table_name).unwrap(),
                };
                TableKey::PrimaryKey(primary)
            }
            else if self.is_unique_key{
                let unique = UniqueKey{
                    name: Some(self.key_name.to_owned()),
                    columns: get_columnname_from_key(em, &self.key_name, table_name).unwrap(),
                };
                TableKey::UniqueKey(unique)
            }
            else if self.is_foreign_key{
                let foreign_key = get_foreign_key(em, &self.key_name, table_name).unwrap();
                TableKey::ForeignKey(foreign_key)
            }
            else {
                panic!("todo on key");
            }
        }
    }

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

    let table_key_simple: Result<Vec<TableKeySimple>, DbError> = 
        em.execute_sql_with_return(&sql, &[&table_name.name, &schema]);
    match table_key_simple{
        Ok(table_key_simple) => {
            let mut table_keys = vec![];
            for simple in table_key_simple{
                let table_key = simple.to_table_key(em, table_name);
                table_keys.push(table_key);
            }
            Ok(table_keys)
        }
        Err(e) => Err(e)
    }
}

/// get the foreign key detail of this key name 
fn get_foreign_key(em: &EntityManager, foreign_key: &String, table_name: &TableName) -> Result<ForeignKey, DbError> {

    #[derive(Debug, FromDao)]
    struct ForeignKeySimple{
        key_name: String,
        foreign_table: String,
        foreign_schema: Option<String>,
    }
    impl ForeignKeySimple{
        fn to_foreign_key(self, columns: Vec<ColumnName>, referred_columns: Vec<ColumnName>) -> ForeignKey {
            ForeignKey{
                name: Some(self.key_name),
                columns: columns,
                foreign_table: TableName{
                    name: self.foreign_table,
                    schema: self.foreign_schema,
                    alias: None
                },
                referred_columns: referred_columns
            }
        }
    }
    let sql = "SELECT conname AS key_name, \
        pg_class.relname AS foreign_table, \
        (SELECT pg_namespace.nspname FROM pg_namespace WHERE pg_namespace.oid = pg_class.relnamespace) AS foreign_schema \
        FROM pg_constraint \
   LEFT JOIN pg_class \
          ON pg_constraint.confrelid = pg_class.oid \
       WHERE pg_constraint.conname = $1
    ";

    let foreign_key_simple: Result<ForeignKeySimple, DbError> = 
        em.execute_sql_with_one_return(&sql, &[&foreign_key]);

    match foreign_key_simple{
        Ok(simple) => {
            let columns: Vec<ColumnName> 
                = get_columnname_from_key(em, foreign_key, table_name)?; 
            let referred_columns: Vec<ColumnName> 
                = get_referred_foreign_columns(em, foreign_key)?;
            let foreign = simple.to_foreign_key(columns, referred_columns);
            Ok(foreign)
        }
        Err(e) => Err(e)
    }
}

fn get_referred_foreign_columns(em: &EntityManager, foreign_key: &String) -> Result<Vec<ColumnName>, DbError> {
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
    match foreign_columns{
        Ok(foreign_columns) => {
            let mut column_names = vec![];
            for simple in foreign_columns{
                let column_name = simple.to_columnname();
                column_names.push(column_name);
            }
            Ok(column_names)
        }
        Err(e) => Err(e)
    }
}


#[cfg(test)]
mod test{

    use super::*;
    use pool::Pool;

    #[test]
    fn table_actor() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let table = TableName::from("actor");
        let table = get_table(&em, &table);
        println!("table: {:#?}", table);
        assert!(table.is_ok());
        assert_eq!(table.unwrap().table_key,
                   vec![TableKey::PrimaryKey(
                            PrimaryKey { 
                                name: Some("actor_pkey".to_string()), 
                                columns: vec![
                                    ColumnName { 
                                        name: "actor_id".to_string(), 
                                        table: None, 
                                        alias: None 
                                    }
                                ]
                            }
                    )]
            );
    }

    #[test]
    fn foreign_key_with_different_referred_column() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let table = TableName::from("store");
        let table = get_table(&em, &table);
        println!("table: {:#?}", table);
        assert!(table.is_ok());
        assert_eq!(table.unwrap().table_key,
                   vec![TableKey::PrimaryKey(PrimaryKey { name: Some("store_pkey".into()), columns: vec![ColumnName { name:
                       "store_id".into(), table: None, alias: None }] }), TableKey::ForeignKey(ForeignKey { name:
                       Some("store_address_id_fkey".into()), columns: vec![ColumnName { name: "address_id".into(),
                       table: None, alias: None }], foreign_table: TableName { name: "address".into(),
                       schema: Some("public".into()), alias: None }, referred_columns: vec![ColumnName { name:
                           "address_id".into(), table: None, alias: None }] }), TableKey::ForeignKey(ForeignKey {
                           name: Some("store_manager_staff_id_fkey".into()), columns: vec![ColumnName { name:
                                     "manager_staff_id".into(), table: None, alias: None }],
                                     foreign_table: TableName { name: "staff".into(), schema:
                                         Some("public".into()), alias: None }, referred_columns:
                               vec![ColumnName { name: "staff_id".into(), table: None, alias: None }] })]
            );
    }

    #[test]
    fn table_film_actor() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let table = TableName::from("film_actor");
        let table = get_table(&em, &table);
        println!("table: {:#?}", table);
        assert!(table.is_ok());
        assert_eq!(table.unwrap().table_key,
                   vec![
                   TableKey::PrimaryKey(
                       PrimaryKey { 
                           name: Some("film_actor_pkey".into()),
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
                                }] 
                       }), 
                   TableKey::ForeignKey(
                       ForeignKey { 
                            name: Some("film_actor_actor_id_fkey".into()), 
                            columns: vec![
                                ColumnName{
                                    name: "actor_id".into(),
                                    table: None,
                                    alias: None,
                                }
                            ], 
                            foreign_table: 
                                TableName {
                                    name: "actor".into(),
                                    schema: Some("public".into()), 
                                    alias: None 
                                }, 
                            referred_columns: vec![
                                ColumnName{
                                    name: "actor_id".into(),
                                    table: None,
                                    alias: None,
                                }
                            ] 
                       }), 
                   TableKey::ForeignKey(
                       ForeignKey { 
                           name: Some("film_actor_film_id_fkey".into()),
                           columns: vec![
                                ColumnName{
                                    name: "film_id".into(),
                                    table: None,
                                    alias: None,
                                }
                            ], 
                           foreign_table: 
                                TableName { 
                                    name: "film".into(), 
                                    schema: Some("public".into()),
                                    alias: None 
                                },
                          referred_columns: vec![
                                ColumnName{
                                    name: "film_id".into(),
                                    table: None,
                                    alias: None,
                                }
                            ] 
                       })
                   ]
            );
    }
}

