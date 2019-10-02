use crate::{
    platform::DBPlatform,
    table::SchemaContent,
    users::{
        Role,
        User,
    },
    DataError,
    Database,
    DatabaseName,
    DbError,
    Table,
    ToValue,
    Value,
};
use log::*;
use rustorm_dao::{
    FromDao,
    TableName,
    ToColumnNames,
    ToDao,
    ToTableName,
};

pub struct EntityManager(pub DBPlatform);

impl EntityManager {
    pub fn set_session_user(&self, username: &str) -> Result<(), DbError> {
        let sql = format!("SET SESSION ROLE '{}'", username);
        self.0.execute_sql_with_return(&sql, &[])?;
        Ok(())
    }

    pub fn get_role(&self, username: &str) -> Result<Option<Role>, DbError> {
        let result = self.0.get_roles(&self, username);
        match result {
            Ok(mut result) => {
                match result.len() {
                    0 => Ok(None),
                    1 => Ok(Some(result.remove(0))),
                    _ => Err(DbError::DataError(DataError::MoreThan1RecordReturned)),
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn db(&self) -> &dyn Database { &*self.0 }

    /// get all the records of this table
    pub fn get_all<T>(&self) -> Result<Vec<T>, DbError>
    where
        T: ToTableName + ToColumnNames + FromDao,
    {
        let table = T::to_table_name();
        let columns = T::to_column_names();
        let enumerated_columns = columns
            .iter()
            .map(|c| c.name.to_owned())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {} FROM {}",
            enumerated_columns,
            table.complete_name()
        );
        let rows = self.0.execute_sql_with_return(&sql, &[])?;
        let mut entities = vec![];
        for dao in rows.iter() {
            let entity = T::from_dao(&dao);
            entities.push(entity)
        }
        Ok(entities)
    }

    /// get the table from database based on this column name
    pub fn get_table(&self, table_name: &TableName) -> Result<Table, DbError> {
        self.0.get_table(self, table_name)
    }

    /// get all the user table and views from the database
    pub fn get_all_tables(&self) -> Result<Vec<Table>, DbError> {
        info!("EXPENSIVE DB OPERATION: get_all_tables");
        self.0.get_all_tables(self)
    }

    /// Get the total count of records
    pub fn get_total_records(&self, table_name: &TableName) -> Result<usize, DbError> {
        #[derive(crate::FromDao)]
        struct Count {
            count: i64,
        }
        let sql = format!(
            "SELECT COUNT(*) AS count FROM {}",
            table_name.complete_name()
        );
        let count: Result<Count, DbError> = self.execute_sql_with_one_return(&sql, &[]);
        count.map(|c| c.count as usize)
    }

    pub fn get_users(&self) -> Result<Vec<User>, DbError> { self.0.get_users(self) }

    pub fn get_database_name(&self) -> Result<Option<DatabaseName>, DbError> {
        self.0.get_database_name(self)
    }

    /// get all table and views grouped per schema
    pub fn get_grouped_tables(&self) -> Result<Vec<SchemaContent>, DbError> {
        self.0.get_grouped_tables(self)
    }

    pub fn insert<T, R>(&self, _entities: &[&T]) -> Result<Vec<R>, DbError>
    where
        T: ToTableName + ToColumnNames + ToDao,
        R: FromDao + ToColumnNames,
    {
        match self.0 {
            #[cfg(feature = "with-sqlite")]
            DBPlatform::Sqlite(_) => self.insert_simple(_entities),
            #[cfg(feature = "with-postgres")]
            DBPlatform::Postgres(_) => self.insert_bulk_with_returning_support(_entities),
        }
    }

    /// called when the platform used is postgresql
    pub fn insert_bulk_with_returning_support<T, R>(
        &self,
        entities: &[&T],
    ) -> Result<Vec<R>, DbError>
    where
        T: ToTableName + ToColumnNames + ToDao,
        R: FromDao + ToColumnNames,
    {
        let columns = T::to_column_names();
        let mut sql = self.build_insert_clause(entities);
        let return_columns = R::to_column_names();
        sql += &self.build_returning_clause(return_columns);

        let mut values: Vec<Value> = Vec::with_capacity(entities.len() * columns.len());
        for entity in entities {
            let dao = entity.to_dao();
            for col in columns.iter() {
                let value = dao.get_value(&col.name);
                match value {
                    Some(value) => values.push(value.clone()),
                    None => values.push(Value::Nil),
                }
            }
        }
        let bvalues: Vec<&Value> = values.iter().collect();
        let rows = self.0.execute_sql_with_return(&sql, &bvalues)?;
        let mut retrieved_entities = vec![];
        for dao in rows.iter() {
            let retrieved = R::from_dao(&dao);
            retrieved_entities.push(retrieved);
        }
        Ok(retrieved_entities)
    }

    /// called multiple times when using database platform that doesn;t support multiple value
    /// insert such as sqlite
    pub fn single_insert<T>(&self, entity: &T) -> Result<(), DbError>
    where
        T: ToTableName + ToColumnNames + ToDao,
    {
        let columns = T::to_column_names();
        let sql = self.build_insert_clause(&[entity]);
        let dao = entity.to_dao();
        let mut values: Vec<Value> = Vec::with_capacity(columns.len());
        for col in columns.iter() {
            let value = dao.get_value(&col.name);
            match value {
                Some(value) => values.push(value.clone()),
                None => values.push(Value::Nil),
            }
        }
        let bvalues: Vec<&Value> = values.iter().collect();
        self.0.execute_sql_with_return(&sql, &bvalues)?;
        Ok(())
    }

    /// this is soly for use with sqlite since sqlite doesn't support bulk insert
    pub fn insert_simple<T, R>(&self, entities: &[&T]) -> Result<Vec<R>, DbError>
    where
        T: ToTableName + ToColumnNames + ToDao,
        R: FromDao + ToColumnNames,
    {
        let return_columns = R::to_column_names();
        let return_column_names = return_columns
            .iter()
            .map(|rc| rc.name.to_owned())
            .collect::<Vec<_>>()
            .join(", ");

        let table = T::to_table_name();
        //TODO: move this specific query to sqlite
        let last_insert_sql = format!(
            "\
             SELECT {} \
             FROM {} \
             WHERE ROWID = (\
             SELECT LAST_INSERT_ROWID() FROM {})",
            return_column_names,
            table.complete_name(),
            table.complete_name()
        );
        let mut retrieved_entities = vec![];
        println!("sql: {}", last_insert_sql);
        for entity in entities {
            self.single_insert(*entity)?;
            let retrieved = self.execute_sql_with_return(&last_insert_sql, &[])?;
            retrieved_entities.extend(retrieved);
        }
        Ok(retrieved_entities)
    }

    /// build the returning clause
    fn build_returning_clause(&self, return_columns: Vec<rustorm_dao::ColumnName>) -> String {
        format!(
            "\nRETURNING \n{}",
            return_columns
                .iter()
                .map(|rc| rc.name.to_owned())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    /// build an insert clause
    fn build_insert_clause<T>(&self, entities: &[&T]) -> String
    where
        T: ToTableName + ToColumnNames + ToDao,
    {
        let table = T::to_table_name();
        let columns = T::to_column_names();
        let columns_len = columns.len();
        let mut sql = String::new();
        sql += &format!("INSERT INTO {} ", table.complete_name());
        sql += &format!(
            "({})\n",
            columns
                .iter()
                .map(|c| c.name.to_owned())
                .collect::<Vec<_>>()
                .join(", ")
        );
        sql += "VALUES ";
        sql += &entities
            .iter()
            .enumerate()
            .map(|(y, _)| {
                format!(
                    "\n\t({})",
                    columns
                        .iter()
                        .enumerate()
                        .map(|(x, _)| format!("${}", y * columns_len + x + 1))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        sql
    }

    #[allow(clippy::redundant_closure)]
    pub fn execute_sql_with_return<'a, R>(
        &self,
        sql: &str,
        params: &[&'a dyn ToValue],
    ) -> Result<Vec<R>, DbError>
    where
        R: FromDao,
    {
        let values: Vec<Value> = params.iter().map(|p| p.to_value()).collect();
        let bvalues: Vec<&Value> = values.iter().collect();
        let rows = self.0.execute_sql_with_return(sql, &bvalues)?;
        Ok(rows.iter().map(|dao| R::from_dao(&dao)).collect::<Vec<R>>())
    }

    pub fn execute_sql_with_one_return<'a, R>(
        &self,
        sql: &str,
        params: &[&'a dyn ToValue],
    ) -> Result<R, DbError>
    where
        R: FromDao,
    {
        let result: Result<Vec<R>, DbError> = self.execute_sql_with_return(sql, &params);
        match result {
            Ok(mut result) => {
                match result.len() {
                    0 => Err(DbError::DataError(DataError::ZeroRecordReturned)),
                    1 => Ok(result.remove(0)),
                    _ => Err(DbError::DataError(DataError::MoreThan1RecordReturned)),
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn execute_sql_with_maybe_one_return<'a, R>(
        &self,
        sql: &str,
        params: &[&'a dyn ToValue],
    ) -> Result<Option<R>, DbError>
    where
        R: FromDao,
    {
        let result: Result<Vec<R>, DbError> = self.execute_sql_with_return(sql, &params);
        match result {
            Ok(mut result) => {
                match result.len() {
                    0 => Ok(None),
                    1 => Ok(Some(result.remove(0))),
                    _ => Err(DbError::DataError(DataError::MoreThan1RecordReturned)),
                }
            }
            Err(e) => Err(e),
        }
    }
}


#[cfg(test)]
#[cfg(feature = "with-postgres")]
mod test_pg {
    use crate::*;
    use chrono::{
        offset::Utc,
        DateTime,
        NaiveDate,
    };
    use log::*;
    use uuid::Uuid;

    #[test]
    fn use_em() {
        #[derive(Debug, FromDao, ToColumnNames, crate::ToTableName)]
        struct Actor {
            actor_id: i32,
            first_name: String,
        }
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url).unwrap();
        let actors: Result<Vec<Actor>, DbError> = em.get_all();
        info!("Actor: {:#?}", actors);
        let actors = actors.unwrap();
        for actor in actors {
            info!("actor: {:?}", actor);
        }
    }

    #[test]
    fn various_data_types() {
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url).unwrap();
        #[derive(Debug, PartialEq, FromDao, ToDao, ToColumnNames, ToTableName)]
        struct Sample {
            vnil: Option<String>,
            vbool: bool,
            vsmallint: i16,
            vint: i32,
            vbigint: i64,
            vfloat: f32,
            vdouble: f64,
            vblob: Vec<u8>,
            vchar: char,
            vtext: String,
            vuuid: Uuid,
            vdate: NaiveDate,
            vtimestamp: DateTime<Utc>,
        }

        let sample: Result<Vec<Sample>, DbError> = em.execute_sql_with_return(
            r#"
            SELECT NULL::TEXT as vnil,
                    true::BOOL as vbool,
                    1000::INT2 as vsmallint,
                    32000::INT as vint,
                    123000::INT4 as vbigint,
                    1.0::FLOAT4 as vfloat,
                    2.0::FLOAT8 as vdouble,
                    E'\\000'::BYTEA as vblob,
                    'c'::CHAR as vchar,
                    'Hello'::TEXT as vtext,
                    'd25af116-fb30-4731-9cf9-2251c235e8fa'::UUID as vuuid,
                    now()::DATE as vdate,
                    now()::TIMESTAMP WITH TIME ZONE as vtimestamp

        "#,
            &[],
        );
        info!("{:#?}", sample);
        assert!(sample.is_ok());

        let sample = sample.unwrap();
        let sample = &sample[0];
        let now = Utc::now();
        let today = now.date();
        let naive_today = today.naive_utc();

        assert_eq!(None, sample.vnil);
        assert_eq!(true, sample.vbool);
        assert_eq!(1000, sample.vsmallint);
        assert_eq!(32000, sample.vint);
        assert_eq!(123000, sample.vbigint);
        assert_eq!(1.0, sample.vfloat);
        assert_eq!(2.0, sample.vdouble);
        assert_eq!(vec![0], sample.vblob);
        assert_eq!('c', sample.vchar);
        assert_eq!("Hello".to_string(), sample.vtext);
        assert_eq!(naive_today, sample.vdate);
        assert_eq!(today, sample.vtimestamp.date());
    }

    #[test]
    fn various_data_types_nulls() {
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url).unwrap();
        #[derive(Debug, PartialEq, FromDao, ToDao, ToColumnNames, ToTableName)]
        struct Sample {
            vnil: Option<String>,
            vbool: Option<bool>,
            vsmallint: Option<i16>,
            vint: Option<i32>,
            vbigint: Option<i64>,
            vfloat: Option<f32>,
            vdouble: Option<f64>,
            vblob: Option<Vec<u8>>,
            vchar: Option<char>,
            vtext: Option<String>,
            vuuid: Option<Uuid>,
            vdate: Option<NaiveDate>,
            vtimestamp: Option<DateTime<Utc>>,
        }

        let sample: Result<Vec<Sample>, DbError> = em.execute_sql_with_return(
            r#"
            SELECT NULL::TEXT as vnil,
                    NULL::BOOL as vbool,
                    NULL::INT2 as vsmallint,
                    NULL::INT as vint,
                    NULL::INT4 as vbigint,
                    NULL::FLOAT4 as vfloat,
                    NULL::FLOAT8 as vdouble,
                    NULL::BYTEA as vblob,
                    NULL::CHAR as vchar,
                    NULL::TEXT as vtext,
                    NULL::UUID as vuuid,
                    NULL::DATE as vdate,
                    NULL::TIMESTAMP WITH TIME ZONE as vtimestamp

        "#,
            &[],
        );
        info!("{:#?}", sample);
        assert!(sample.is_ok());

        let sample = sample.unwrap();
        let sample = &sample[0];

        assert_eq!(None, sample.vnil);
        assert_eq!(None, sample.vbool);
        assert_eq!(None, sample.vsmallint);
        assert_eq!(None, sample.vint);
        assert_eq!(None, sample.vbigint);
        assert_eq!(None, sample.vfloat);
        assert_eq!(None, sample.vdouble);
        assert_eq!(None, sample.vblob);
        assert_eq!(None, sample.vtext);
        assert_eq!(None, sample.vdate);
        assert_eq!(None, sample.vtimestamp);
    }

    #[test]
    fn edgecase_use_char_as_string() {
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url).unwrap();
        #[derive(Debug, PartialEq, FromDao, ToDao, ToColumnNames, ToTableName)]
        struct Sample {
            vchar: String,
        }

        let sample: Result<Vec<Sample>, DbError> = em.execute_sql_with_return(
            r#"
            SELECT
                'c'::CHAR as VCHAR
        "#,
            &[],
        );
        info!("{:#?}", sample);
        assert!(sample.is_ok());

        let sample = sample.unwrap();
        let sample = &sample[0];
        assert_eq!("c".to_string(), sample.vchar);
    }

    #[test]
    fn char1() {
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url).unwrap();
        #[derive(Debug, PartialEq, FromDao, ToDao, ToColumnNames, ToTableName)]
        struct Sample {
            vchar: char,
        }

        let sample: Result<Vec<Sample>, DbError> = em.execute_sql_with_return(
            r#"
            SELECT
                'c'::CHAR as VCHAR
        "#,
            &[],
        );
        info!("{:#?}", sample);
        assert!(sample.is_ok());

        let sample = sample.unwrap();
        let sample = &sample[0];
        assert_eq!('c', sample.vchar);
    }

    #[test]
    fn insert_some_data() {
        #[derive(Debug, PartialEq, FromDao, ToDao, ToColumnNames, ToTableName)]
        struct Actor {
            first_name: String,
            last_name: String,
        }
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url).unwrap();
        let tom_cruise = Actor {
            first_name: "TOM".into(),
            last_name: "CRUISE".to_string(),
        };
        let tom_hanks = Actor {
            first_name: "TOM".into(),
            last_name: "HANKS".to_string(),
        };

        let actors: Result<Vec<Actor>, DbError> = em.insert(&[&tom_cruise, &tom_hanks]);
        info!("Actor: {:#?}", actors);
        assert!(actors.is_ok());
        let actors = actors.unwrap();
        assert_eq!(tom_cruise, actors[0]);
        assert_eq!(tom_hanks, actors[1]);
    }

    #[test]
    fn insert_some_data_with_different_retrieve() {
        mod for_insert {
            use super::*;
            #[derive(Debug, PartialEq, ToDao, ToColumnNames, ToTableName)]
            pub struct Actor {
                pub first_name: String,
                pub last_name: String,
            }
        }

        mod for_retrieve {
            use super::*;
            #[derive(Debug, FromDao, ToColumnNames, ToTableName)]
            pub struct Actor {
                pub actor_id: i32,
                pub first_name: String,
                pub last_name: String,
                pub last_update: DateTime<Utc>,
            }
        }

        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url).unwrap();
        let tom_cruise = for_insert::Actor {
            first_name: "TOM".into(),
            last_name: "CRUISE".to_string(),
        };
        let tom_hanks = for_insert::Actor {
            first_name: "TOM".into(),
            last_name: "HANKS".to_string(),
        };

        let actors: Result<Vec<for_retrieve::Actor>, DbError> =
            em.insert(&[&tom_cruise, &tom_hanks]);
        info!("Actor: {:#?}", actors);
        assert!(actors.is_ok());
        let actors = actors.unwrap();
        let today = Utc::now().date();
        assert_eq!(tom_cruise.first_name, actors[0].first_name);
        assert_eq!(tom_cruise.last_name, actors[0].last_name);
        assert_eq!(today, actors[0].last_update.date());
        assert_eq!(tom_hanks.first_name, actors[1].first_name);
        assert_eq!(tom_hanks.last_name, actors[1].last_name);
        assert_eq!(today, actors[1].last_update.date());
    }

    #[test]
    fn execute_sql_non_existing_table() {
        #[derive(Debug, crate::FromDao)]
        struct Event {
            id: i32,
            name: String,
            created: DateTime<Utc>,
        }
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url).unwrap();
        let id = 1;
        let name = "dbus-notifications".to_string();
        let created = Utc::now();
        let events: Result<Vec<Event>, DbError> = em.execute_sql_with_return(
            "SELECT $1::INT as id, $2::TEXT as name, $3::TIMESTAMP WITH TIME ZONE as created",
            &[&id, &name, &created],
        );
        info!("events: {:#?}", events);
        assert!(events.is_ok());
        for event in events.unwrap().iter() {
            assert_eq!(event.id, id);
            assert_eq!(event.name, name);
            assert_eq!(event.created.date(), created.date());
        }
    }

    #[test]
    fn get_table() {
        let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url).unwrap();
        let actor = TableName::from("actor");
        let table = em.db().get_table(&em, &actor);
        assert!(table.is_ok());
    }
}
