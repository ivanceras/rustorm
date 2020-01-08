use crate::{
    types::SqlType,
    Column,
    ColumnName,
    TableName,
};

#[derive(Debug, PartialEq, Clone)]
pub struct Table {
    pub name: TableName,

    /// comment of this table
    pub comment: Option<String>,

    /// columns of this table
    pub columns: Vec<Column>,

    /// views can also be generated
    pub is_view: bool,

    pub table_key: Vec<TableKey>,
}

impl Table {
    pub fn complete_name(&self) -> String { self.name.complete_name() }

    pub fn safe_name(&self) -> String { self.name.safe_name() }

    pub fn safe_complete_name(&self) -> String { self.name.safe_complete_name() }

    pub fn get_primary_column_names(&self) -> Vec<&ColumnName> {
        let mut primary: Vec<&ColumnName> = vec![];
        for key in &self.table_key {
            if let TableKey::PrimaryKey(ref pk) = key {
                for col in &pk.columns {
                    primary.push(col)
                }
            }
        }
        primary.sort_by(|a, b| a.name.cmp(&b.name));
        primary
    }

    pub fn get_non_primary_columns(&self) -> Vec<&Column> {
        let primary = self.get_primary_columns();
        self.columns
            .iter()
            .filter(|c| !primary.contains(c))
            .collect()
    }

    pub fn get_primary_columns(&self) -> Vec<&Column> {
        self.get_primary_column_names()
            .iter()
            .filter_map(|column_name| self.get_column(column_name))
            .collect()
    }

    pub fn get_primary_column_types(&self) -> Vec<&SqlType> {
        self.get_primary_columns()
            .iter()
            .map(|column| &column.specification.sql_type)
            .collect()
    }

    pub fn get_foreign_keys(&self) -> Vec<&ForeignKey> {
        let mut foreign: Vec<&ForeignKey> = vec![];
        for key in &self.table_key {
            if let TableKey::ForeignKey(ref fk) = key {
                foreign.push(fk)
            }
        }
        foreign
    }

    pub fn get_foreign_key_to_table(&self, table_name: &TableName) -> Option<&ForeignKey> {
        let foreign_keys: Vec<&ForeignKey> = self.get_foreign_keys();
        for fk in foreign_keys {
            if fk.foreign_table == *table_name {
                return Some(fk);
            }
        }
        None
    }

    /// get the (local_columns, foreign_columns) to the table
    pub fn get_local_foreign_columns_pair_to_table(
        &self,
        table_name: &TableName,
    ) -> Vec<(&ColumnName, &ColumnName)> {
        let foreign_keys: Vec<&ForeignKey> = self.get_foreign_keys();
        for fk in foreign_keys {
            if fk.foreign_table == *table_name {
                let mut container = vec![];
                for (local_column, referred_column) in
                    fk.columns.iter().zip(fk.referred_columns.iter())
                {
                    container.push((local_column, referred_column));
                }
                return container;
            }
        }
        vec![]
    }

    fn get_foreign_columns_to_table(&self, table_name: &TableName) -> Vec<&Column> {
        self.get_foreign_column_names_to_table(table_name)
            .iter()
            .filter_map(|column_name| self.get_column(column_name))
            .collect()
    }

    pub fn get_foreign_column_types_to_table(&self, table_name: &TableName) -> Vec<&SqlType> {
        self.get_foreign_columns_to_table(table_name)
            .iter()
            .map(|column| &column.specification.sql_type)
            .collect()
    }

    pub fn get_foreign_column_names_to_table(&self, table_name: &TableName) -> Vec<&ColumnName> {
        let mut foreign_columns = vec![];
        let foreign_keys = self.get_foreign_key_to_table(table_name);
        for fk in &foreign_keys {
            for fk_column in &fk.columns {
                foreign_columns.push(fk_column);
            }
        }
        foreign_columns
    }

    ///
    pub fn get_foreign_column_names(&self) -> Vec<&ColumnName> {
        let mut foreign_columns = vec![];
        let foreign_keys = self.get_foreign_keys();
        for fk in &foreign_keys {
            for fk_column in &fk.columns {
                foreign_columns.push(fk_column);
            }
        }
        foreign_columns
    }

    /// return the local columns of this table
    /// that is referred by the argument table name
    pub fn get_referred_columns_to_table(
        &self,
        table_name: &TableName,
    ) -> Option<&Vec<ColumnName>> {
        let foreign_keys: Vec<&ForeignKey> = self.get_foreign_keys();
        for fk in foreign_keys {
            if fk.foreign_table == *table_name {
                return Some(&fk.referred_columns);
            }
        }
        None
    }

    pub fn get_column(&self, column_name: &ColumnName) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == *column_name)
    }
}

/// example:
///     category { id, name }
///     product { product_id, name, category_id }
///
/// if the table in context is product and the foreign table is category
/// ForeignKey{
///     name: product_category_fkey
///     columns: _category_id_
///     foreign_table: category
///     referred_columns: _id_
/// }
#[derive(Debug, PartialEq, Clone)]
pub struct ForeignKey {
    pub name: Option<String>,
    // the local columns of this table local column = foreign_column
    pub columns: Vec<ColumnName>,
    // referred foreign table
    pub foreign_table: TableName,
    // referred column of the foreign table
    // this is most likely the primary key of the table in context
    pub referred_columns: Vec<ColumnName>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Key {
    pub name: Option<String>,
    pub columns: Vec<ColumnName>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum TableKey {
    PrimaryKey(Key),
    UniqueKey(Key),
    Key(Key),
    ForeignKey(ForeignKey),
}

#[derive(Debug)]
pub struct SchemaContent {
    pub schema: String,
    pub tablenames: Vec<TableName>,
    pub views: Vec<TableName>,
}

#[cfg(test)]
#[cfg(feature = "with-postgres")]
mod test {
    use crate::{
        table::*,
        *,
    };
    use log::*;

    #[test]
    fn referred_columns() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        let mut db = pool.db(db_url).unwrap();
        assert!(em.is_ok());
        let film_tablename = TableName::from("public.film");
        let film = db.get_table(&film_tablename);
        let film_actor_tablename = TableName::from("public.film_actor");
        let film_actor = db.get_table(&film_actor_tablename);
        assert!(film.is_ok());
        info!("film: {:#?}", film);
        info!("FILM ACTOR {:#?}", film_actor);
        let film = film.unwrap();
        let film_actor = film_actor.unwrap();
        let rc = film_actor.get_referred_columns_to_table(&film.name);
        info!("rc: {:#?}", rc);
        assert_eq!(
            rc,
            Some(&vec![ColumnName {
                name: "film_id".to_string(),
                table: None,
                alias: None,
            }])
        );
    }

    #[test]
    fn referred_columns_hero_id() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/dota";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let mut em = em.unwrap();
        let hero_tablename = TableName::from("public.hero");
        let hero = em.get_table(&hero_tablename);
        let hero_ability_tablename = TableName::from("public.hero_ability");
        let hero_ability = em.get_table(&hero_ability_tablename);
        assert!(hero.is_ok());
        info!("hero {:#?}", hero);
        info!("hero ability {:#?}", hero_ability);
        let hero = hero.unwrap();
        let hero_ability = hero_ability.unwrap();
        let rc = hero_ability.get_referred_columns_to_table(&hero.name);
        info!("rc: {:#?}", rc);
        assert_eq!(
            rc,
            Some(&vec![ColumnName {
                name: "id".to_string(),
                table: None,
                alias: None,
            }])
        );
        let foreign_key = hero_ability.get_foreign_key_to_table(&hero.name);
        info!("foreign_key: {:#?}", foreign_key);
        assert_eq!(
            foreign_key,
            Some(&ForeignKey {
                name: Some("hero_id_fkey".to_string()),
                columns: vec![ColumnName {
                    name: "hero_id".to_string(),
                    table: None,
                    alias: None,
                }],
                foreign_table: TableName {
                    name: "hero".to_string(),
                    schema: Some("public".to_string()),
                    alias: None,
                },
                referred_columns: vec![ColumnName {
                    name: "id".to_string(),
                    table: None,
                    alias: None,
                }],
            })
        );
    }
}
