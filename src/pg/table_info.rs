//! module table_info extract the table meta data using SQL queries on pg_catalog.
//! This is not using information_schema since there is a performance issue with it.
use crate::{
    pg::column_info,
    table::{
        self,
        ForeignKey,
        Key,
        SchemaContent,
        Table,
        TableKey,
    },
    Column,
    ColumnName,
    Database,
    DbError,
    FromDao,
    TableName,
    Value,
};
use log::*;
use rustorm_dao::value::ToValue;

/// get all database tables and views except from special schema
pub fn get_all_tables(db: &mut dyn Database) -> Result<Vec<Table>, DbError> {
    #[derive(Debug, FromDao)]
    struct TableNameSimple {
        name: String,
        schema: String,
    }
    impl TableNameSimple {
        fn to_tablename(&self) -> TableName {
            TableName {
                name: self.name.to_string(),
                schema: Some(self.schema.to_string()),
                alias: None,
            }
        }
    }
    let sql = r#"SELECT
             pg_class.relname AS name,
             pg_namespace.nspname AS schema
        FROM pg_class
   LEFT JOIN pg_namespace
          ON pg_namespace.oid = pg_class.relnamespace
       WHERE
             pg_class.relkind IN ('r','v')
         AND pg_namespace.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
         AND (has_table_privilege(pg_class.oid, 'SELECT')
                OR has_any_column_privilege(pg_class.oid, 'SELECT')
             )
    ORDER BY nspname, relname
            "#;

    let rows = db.execute_sql_with_return(sql, &[]);
    let tablenames_simple: Result<Vec<TableNameSimple>, DbError> = rows.map(|rows| {
        rows.iter()
            .map(|row| {
                let name: String = row.get("name").expect("must have table name");
                let schema: String = row.get("schema").expect("must have schema");
                TableNameSimple { name, schema }
            })
            .collect()
    });
    match tablenames_simple {
        Ok(simples) => {
            let mut tables = Vec::with_capacity(simples.len());
            for simple in simples {
                let tablename = simple.to_tablename();
                info!("  {}", tablename.complete_name());
                let table: Result<Table, DbError> = get_table(db, &tablename);
                match table {
                    Ok(table) => {
                        tables.push(table);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Ok(tables)
        }
        Err(e) => Err(e),
    }
}

enum TableKind {
    Table,
    View,
}
impl TableKind {
    fn to_sql_char(&self) -> char {
        match *self {
            TableKind::Table => 'r',
            TableKind::View => 'v',
        }
    }
}

/// get all database tables or views from this schema
fn get_schema_tables(
    db: &mut dyn Database,
    schema: &str,
    kind: &TableKind,
) -> Result<Vec<TableName>, DbError> {
    #[derive(Debug, FromDao)]
    struct TableNameSimple {
        name: String,
        schema: String,
    }
    impl TableNameSimple {
        fn to_tablename(&self) -> TableName {
            TableName {
                name: self.name.to_string(),
                schema: Some(self.schema.to_string()),
                alias: None,
            }
        }
    }
    let sql = r#"SELECT
             pg_class.relname AS name,
             pg_namespace.nspname AS schema
        FROM pg_class
   LEFT JOIN pg_namespace
          ON pg_namespace.oid = pg_class.relnamespace
       WHERE
             pg_class.relkind = $2::char
         AND pg_namespace.nspname = $1
    ORDER BY relname
            "#;
    let tablenames_simple: Result<Vec<TableNameSimple>, DbError> = db
        .execute_sql_with_return(sql, &[
            &Value::Text(schema.to_string()),
            &Value::Char(kind.to_sql_char()),
        ])
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    let name: String = row.get("name").expect("must have table name");
                    let schema: String = row.get("schema").expect("must have schema");
                    TableNameSimple { name, schema }
                })
                .collect()
        });
    match tablenames_simple {
        Ok(simples) => {
            let mut table_names = Vec::with_capacity(simples.len());
            for simple in simples {
                table_names.push(simple.to_tablename());
            }
            Ok(table_names)
        }
        Err(e) => Err(e),
    }
}

/// get all user created schema
/// special tables such as: information_schema, pg_catalog, pg_toast, pg_temp_1, pg_toast_temp_1,
/// etc. are excluded
fn get_schemas(db: &mut dyn Database) -> Result<Vec<String>, DbError> {
    let sql = r#"SELECT
             pg_namespace.nspname AS schema
        FROM pg_namespace
       WHERE
             pg_namespace.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
         AND pg_namespace.nspname NOT LIKE 'pg_temp_%'
         AND pg_namespace.nspname NOT LIKE 'pg_toast_temp_%'
    ORDER BY nspname
            "#;
    db.execute_sql_with_return(sql, &[]).map(|rows| {
        rows.iter()
            .map(|row| row.get("schema").expect("must have schema"))
            .collect()
    })
}

/// get the table and views of this database organized per schema
pub fn get_organized_tables(db: &mut dyn Database) -> Result<Vec<SchemaContent>, DbError> {
    let schemas = get_schemas(db);
    match schemas {
        Ok(schemas) => {
            let mut contents = Vec::with_capacity(schemas.len());
            for schema in schemas {
                let tables = get_schema_tables(db, &schema, &TableKind::Table)?;
                let views = get_schema_tables(db, &schema, &TableKind::View)?;
                info!("views: {:#?}", views);
                contents.push(SchemaContent {
                    schema: schema.to_string(),
                    tablenames: tables,
                    views,
                });
            }
            Ok(contents)
        }
        Err(e) => Err(e),
    }
}

/// get the table definition, its columns and table_keys
pub fn get_table(db: &mut dyn Database, table_name: &TableName) -> Result<Table, DbError> {
    #[derive(Debug, FromDao)]
    struct TableSimple {
        name: String,
        schema: String,
        comment: Option<String>,
        is_view: bool,
    }

    impl TableSimple {
        fn to_table(&self, columns: Vec<Column>, table_key: Vec<TableKey>) -> Table {
            Table {
                name: TableName {
                    name: self.name.to_string(),
                    schema: Some(self.schema.to_string()),
                    alias: None,
                },
                comment: self.comment.clone(),
                columns,
                is_view: self.is_view,
                table_key,
            }
        }
    }

    let sql = r#"SELECT pg_class.relname as name,
                pg_namespace.nspname as schema,
   CASE WHEN pg_class.relkind = 'v' THEN true ELSE false
         END AS is_view,
                obj_description(pg_class.oid) as comment
        FROM pg_class
   LEFT JOIN pg_namespace
          ON pg_namespace.oid = pg_class.relnamespace
       WHERE pg_class.relname = $1
         AND pg_namespace.nspname = $2
    "#;

    let schema = match table_name.schema {
        Some(ref schema) => schema.to_string(),
        None => "public".to_string(),
    };

    let mut table_simples: Vec<TableSimple> = db
        .execute_sql_with_return(&sql, &[
            &Value::Text(table_name.name.to_string()),
            &Value::Text(schema),
        ])
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    TableSimple {
                        name: row.get("name").expect("must have a table name"),
                        schema: row.get("schema").expect("must have a schema"),
                        comment: row.get_opt("comment").expect("must not error"),
                        is_view: row.get("is_view").expect("must have is_view"),
                    }
                })
                .collect()
        })?;
    assert_eq!(table_simples.len(), 1);
    let table_simple = table_simples.remove(0);
    let columns: Vec<Column> = column_info::get_columns(db, table_name)?;
    let keys: Vec<TableKey> = get_table_key(db, table_name)?;
    let table: Table = table_simple.to_table(columns, keys);
    Ok(table)
}

/// column name only
#[derive(Debug, FromDao)]
struct ColumnNameSimple {
    column: String,
}
impl ColumnNameSimple {
    fn to_columnname(&self) -> ColumnName {
        ColumnName {
            name: self.column.to_string(),
            table: None,
            alias: None,
        }
    }
}

/// get the column names involved in a Primary key or unique key
fn get_columnname_from_key(
    db: &mut dyn Database,
    key_name: &str,
    table_name: &TableName,
) -> Result<Vec<ColumnName>, DbError> {
    let sql = r#"SELECT pg_attribute.attname as column
        FROM pg_attribute
        JOIN pg_class
          ON pg_class.oid = pg_attribute.attrelid
   LEFT JOIN pg_namespace
          ON pg_namespace.oid = pg_class.relnamespace
   LEFT JOIN pg_constraint
          ON pg_constraint.conrelid = pg_class.oid
         AND pg_attribute.attnum = ANY (pg_constraint.conkey)
       WHERE pg_namespace.nspname = $3
         AND pg_class.relname = $2
         AND pg_attribute.attnum > 0
         AND pg_constraint.conname = $1
        "#;
    let schema = match table_name.schema {
        Some(ref schema) => schema.to_string(),
        None => "public".to_string(),
    };

    let column_name_simple: Result<Vec<ColumnNameSimple>, DbError> = db
        .execute_sql_with_return(&sql, &[
            &key_name.to_value(),
            &table_name.name.to_value(),
            &schema.to_value(),
        ])
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    ColumnNameSimple {
                        column: row.get("column").expect("a column"),
                    }
                })
                .collect()
        });
    match column_name_simple {
        Ok(column_name_simple) => {
            let mut column_names = vec![];
            for simple in column_name_simple {
                let column_name = simple.to_columnname();
                column_names.push(column_name);
            }
            Ok(column_names)
        }
        Err(e) => Err(e),
    }
}

/// get the Primary keys, Unique keys of this table
fn get_table_key(db: &mut dyn Database, table_name: &TableName) -> Result<Vec<TableKey>, DbError> {
    #[derive(Debug, FromDao)]
    struct TableKeySimple {
        key_name: String,
        is_primary_key: bool,
        is_unique_key: bool,
        is_foreign_key: bool,
    }

    impl TableKeySimple {
        fn to_table_key(&self, db: &mut dyn Database, table_name: &TableName) -> TableKey {
            if self.is_primary_key {
                let primary = Key {
                    name: Some(self.key_name.to_owned()),
                    columns: get_columnname_from_key(db, &self.key_name, table_name).unwrap(),
                };
                TableKey::PrimaryKey(primary)
            } else if self.is_unique_key {
                let unique = Key {
                    name: Some(self.key_name.to_owned()),
                    columns: get_columnname_from_key(db, &self.key_name, table_name).unwrap(),
                };
                TableKey::UniqueKey(unique)
            } else if self.is_foreign_key {
                let foreign_key = get_foreign_key(db, &self.key_name, table_name).unwrap();
                TableKey::ForeignKey(foreign_key)
            } else {
                let key = table::Key {
                    name: Some(self.key_name.to_owned()),
                    columns: get_columnname_from_key(db, &self.key_name, table_name).unwrap(),
                };
                TableKey::Key(key)
            }
        }
    }

    let sql = r#"SELECT conname AS key_name,
        CASE WHEN contype = 'p' THEN true ELSE false END AS is_primary_key,
        CASE WHEN contype = 'u' THEN true ELSE false END AS is_unique_key,
        CASE WHEN contype = 'f' THEN true ELSE false END AS is_foreign_key
        FROM pg_constraint
   LEFT JOIN pg_class
          ON pg_class.oid = pg_constraint.conrelid
   LEFT JOIN pg_namespace
          ON pg_namespace.oid = pg_class.relnamespace
   LEFT JOIN pg_class AS g
          ON pg_constraint.confrelid = g.oid
       WHERE pg_class.relname = $1
         AND pg_namespace.nspname = $2
    ORDER BY is_primary_key DESC, is_unique_key DESC, is_foreign_key DESC
    "#;

    let schema = match table_name.schema {
        Some(ref schema) => schema.to_string(),
        None => "public".to_string(),
    };

    let table_key_simple: Result<Vec<TableKeySimple>, DbError> = db
        .execute_sql_with_return(&sql, &[&table_name.name.to_value(), &schema.to_value()])
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    TableKeySimple {
                        key_name: row.get("key_name").expect("a key_name"),
                        is_primary_key: row.get("is_primary_key").expect("is_primary_key"),
                        is_unique_key: row.get("is_unique_key").expect("is_unique_key"),
                        is_foreign_key: row.get("is_foreign_key").expect("is_foreign_key"),
                    }
                })
                .collect()
        });
    match table_key_simple {
        Ok(table_key_simple) => {
            let mut table_keys = vec![];
            for simple in table_key_simple {
                let table_key = simple.to_table_key(db, table_name);
                table_keys.push(table_key);
            }
            Ok(table_keys)
        }
        Err(e) => Err(e),
    }
}

/// get the foreign key detail of this key name
fn get_foreign_key(
    db: &mut dyn Database,
    foreign_key: &str,
    table_name: &TableName,
) -> Result<ForeignKey, DbError> {
    #[derive(Debug, FromDao)]
    struct ForeignKeySimple {
        key_name: String,
        foreign_table: String,
        foreign_schema: Option<String>,
    }
    impl ForeignKeySimple {
        fn to_foreign_key(
            &self,
            columns: Vec<ColumnName>,
            referred_columns: Vec<ColumnName>,
        ) -> ForeignKey {
            ForeignKey {
                name: Some(self.key_name.to_string()),
                columns,
                foreign_table: TableName {
                    name: self.foreign_table.to_string(),
                    schema: self.foreign_schema.clone(),
                    alias: None,
                },
                referred_columns,
            }
        }
    }
    let sql = r#"SELECT DISTINCT conname AS key_name,
        pg_class.relname AS foreign_table,
        (SELECT pg_namespace.nspname FROM pg_namespace WHERE pg_namespace.oid = pg_class.relnamespace) AS foreign_schema
        FROM pg_constraint
   LEFT JOIN pg_class
          ON pg_constraint.confrelid = pg_class.oid
       WHERE pg_constraint.conname = $1
    "#;

    let foreign_key_simple: Result<Vec<ForeignKeySimple>, DbError> = db
        .execute_sql_with_return(&sql, &[&foreign_key.to_value()])
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    ForeignKeySimple {
                        key_name: row.get("key_name").expect("key_name"),
                        foreign_table: row.get("foreign_table").expect("foreign_table"),
                        foreign_schema: row.get_opt("foreign_schema").expect("foreign_schema"),
                    }
                })
                .collect()
        });
    match foreign_key_simple {
        Ok(mut simple) => {
            assert_eq!(simple.len(), 1);
            let simple = simple.remove(0);
            let columns: Vec<ColumnName> = get_columnname_from_key(db, foreign_key, table_name)?;
            let referred_columns: Vec<ColumnName> = get_referred_foreign_columns(db, foreign_key)?;
            let foreign = simple.to_foreign_key(columns, referred_columns);
            Ok(foreign)
        }
        Err(e) => Err(e),
    }
}

fn get_referred_foreign_columns(
    db: &mut dyn Database,
    foreign_key: &str,
) -> Result<Vec<ColumnName>, DbError> {
    let sql = r#"SELECT DISTINCT conname AS key_name,
        pg_attribute.attname AS column
        FROM pg_constraint
   LEFT JOIN pg_class
          ON pg_constraint.confrelid = pg_class.oid
   LEFT JOIN pg_attribute
          ON pg_attribute.attnum = ANY (pg_constraint.confkey)
         AND pg_class.oid = pg_attribute.attrelid
       WHERE pg_constraint.conname = $1
    "#;

    let foreign_columns: Result<Vec<ColumnNameSimple>, DbError> = db
        .execute_sql_with_return(&sql, &[&foreign_key.to_value()])
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    ColumnNameSimple {
                        column: row.get("column").expect("a column"),
                    }
                })
                .collect()
        });
    match foreign_columns {
        Ok(foreign_columns) => {
            let mut column_names = vec![];
            for simple in foreign_columns {
                let column_name = simple.to_columnname();
                column_names.push(column_name);
            }
            Ok(column_names)
        }
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod test {

    use crate::{
        pg::table_info::*,
        Pool,
        TableName,
    };

    #[test]
    fn all_schemas() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let db = pool.db(db_url);
        assert!(db.is_ok());
        let mut db = db.unwrap();
        let schemas = get_schemas(&mut *db);
        info!("schemas: {:#?}", schemas);
        assert!(schemas.is_ok());
        let schemas = schemas.unwrap();
        assert_eq!(schemas, vec!["public"]);
    }

    #[test]
    fn all_tables() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let db = pool.db(db_url);
        assert!(db.is_ok());
        let mut db = db.unwrap();
        let tables = get_all_tables(&mut *db);
        info!("tables: {:#?}", tables);
        assert!(tables.is_ok());
        assert_eq!(30, tables.unwrap().len());
    }

    #[test]
    fn table_actor() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let db = pool.db(db_url);
        assert!(db.is_ok());
        let mut db = db.unwrap();
        let table = TableName::from("actor");
        let table = get_table(&mut *db, &table);
        info!("table: {:#?}", table);
        assert!(table.is_ok());
        assert_eq!(table.unwrap().table_key, vec![TableKey::PrimaryKey(Key {
            name: Some("actor_pkey".to_string()),
            columns: vec![ColumnName {
                name: "actor_id".to_string(),
                table: None,
                alias: None,
            }],
        })]);
    }

    #[test]
    fn foreign_key_with_different_referred_column() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let db = pool.db(db_url);
        assert!(db.is_ok());
        let mut db = db.unwrap();
        let table = TableName::from("store");
        let table = get_table(&mut *db, &table);
        info!("table: {:#?}", table);
        assert!(table.is_ok());
        assert_eq!(table.unwrap().table_key, vec![
            TableKey::PrimaryKey(Key {
                name: Some("store_pkey".into()),
                columns: vec![ColumnName {
                    name: "store_id".into(),
                    table: None,
                    alias: None,
                }],
            }),
            TableKey::ForeignKey(ForeignKey {
                name: Some("store_address_id_fkey".into()),
                columns: vec![ColumnName {
                    name: "address_id".into(),
                    table: None,
                    alias: None,
                }],
                foreign_table: TableName {
                    name: "address".into(),
                    schema: Some("public".into()),
                    alias: None,
                },
                referred_columns: vec![ColumnName {
                    name: "address_id".into(),
                    table: None,
                    alias: None,
                }],
            }),
            TableKey::ForeignKey(ForeignKey {
                name: Some("store_manager_staff_id_fkey".into()),
                columns: vec![ColumnName {
                    name: "manager_staff_id".into(),
                    table: None,
                    alias: None,
                }],
                foreign_table: TableName {
                    name: "staff".into(),
                    schema: Some("public".into()),
                    alias: None,
                },
                referred_columns: vec![ColumnName {
                    name: "staff_id".into(),
                    table: None,
                    alias: None,
                }],
            }),
        ]);
    }

    #[test]
    fn table_film_actor() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let db = pool.db(db_url);
        assert!(db.is_ok());
        let mut db = db.unwrap();
        let table = TableName::from("film_actor");
        let table = get_table(&mut *db, &table);
        info!("table: {:#?}", table);
        assert!(table.is_ok());
        assert_eq!(table.unwrap().table_key, vec![
            TableKey::PrimaryKey(Key {
                name: Some("film_actor_pkey".into()),
                columns: vec![
                    ColumnName {
                        name: "actor_id".into(),
                        table: None,
                        alias: None,
                    },
                    ColumnName {
                        name: "film_id".into(),
                        table: None,
                        alias: None,
                    },
                ],
            }),
            TableKey::ForeignKey(ForeignKey {
                name: Some("film_actor_actor_id_fkey".into()),
                columns: vec![ColumnName {
                    name: "actor_id".into(),
                    table: None,
                    alias: None,
                }],
                foreign_table: TableName {
                    name: "actor".into(),
                    schema: Some("public".into()),
                    alias: None,
                },
                referred_columns: vec![ColumnName {
                    name: "actor_id".into(),
                    table: None,
                    alias: None,
                }],
            }),
            TableKey::ForeignKey(ForeignKey {
                name: Some("film_actor_film_id_fkey".into()),
                columns: vec![ColumnName {
                    name: "film_id".into(),
                    table: None,
                    alias: None,
                }],
                foreign_table: TableName {
                    name: "film".into(),
                    schema: Some("public".into()),
                    alias: None,
                },
                referred_columns: vec![ColumnName {
                    name: "film_id".into(),
                    table: None,
                    alias: None,
                }],
            }),
        ]);
    }

    #[test]
    fn composite_foreign_key() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let db = pool.db(db_url);
        assert!(db.is_ok());
        let mut db = db.unwrap();
        let table = TableName::from("film_actor_awards");
        let table = get_table(&mut *db, &table);
        info!("table: {:#?}", table);
        assert!(table.is_ok());
        assert_eq!(table.unwrap().table_key, vec![
            TableKey::PrimaryKey(Key {
                name: Some("film_actor_awards_pkey".into()),
                columns: vec![
                    ColumnName {
                        name: "actor_id".into(),
                        table: None,
                        alias: None,
                    },
                    ColumnName {
                        name: "film_id".into(),
                        table: None,
                        alias: None,
                    },
                    ColumnName {
                        name: "award".into(),
                        table: None,
                        alias: None,
                    },
                ],
            }),
            TableKey::ForeignKey(ForeignKey {
                name: Some("film_actor_awards_actor_id_film_id_fkey".into()),
                columns: vec![
                    ColumnName {
                        name: "actor_id".into(),
                        table: None,
                        alias: None,
                    },
                    ColumnName {
                        name: "film_id".into(),
                        table: None,
                        alias: None,
                    },
                ],
                foreign_table: TableName {
                    name: "film_actor".into(),
                    schema: Some("public".into()),
                    alias: None,
                },
                referred_columns: vec![
                    ColumnName {
                        name: "actor_id".into(),
                        table: None,
                        alias: None,
                    },
                    ColumnName {
                        name: "film_id".into(),
                        table: None,
                        alias: None,
                    },
                ],
            }),
        ]);
    }

    #[test]
    fn organized_content() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let db = pool.db(db_url);
        assert!(db.is_ok());
        let mut db = db.unwrap();
        let organized = get_organized_tables(&mut *db);
        //info!("organized: {:#?}", organized);
        assert!(organized.is_ok());
        let organized = organized.unwrap();
        assert_eq!(organized.len(), 1);
        assert_eq!(organized[0].schema, "public");
        assert_eq!(organized[0].tablenames.len(), 23);
        assert_eq!(organized[0].views.len(), 7);
    }
}
