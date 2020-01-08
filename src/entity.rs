use crate::{
    table::SchemaContent,
    users::{
        Role,
        User,
    },
    DBPlatform,
    DataError,
    Database,
    DatabaseName,
    DbError,
    Table,
    ToValue,
    Value,
};

use rustorm_dao::{
    FromDao,
    TableName,
    ToColumnNames,
    ToDao,
    ToTableName,
};


pub struct EntityManager(pub DBPlatform);

impl EntityManager {
    pub fn set_session_user(&mut self, username: &str) -> Result<(), DbError> {
        let sql = format!("SET SESSION ROLE '{}'", username);
        self.0.execute_sql_with_return(&sql, &[])?;
        Ok(())
    }

    pub fn get_role(&mut self, username: &str) -> Result<Option<Role>, DbError> {
        let result = self.0.get_roles(username);
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

    pub fn db(&mut self) -> &mut dyn Database { &mut *self.0 }

    /// get all the records of this table
    pub fn get_all<T>(&mut self) -> Result<Vec<T>, DbError>
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
    pub fn get_table(&mut self, table_name: &TableName) -> Result<Table, DbError> {
        self.0.get_table(table_name)
    }

    /// get all the user table and views from the database
    pub fn get_all_tables(&mut self) -> Result<Vec<Table>, DbError> {
        info!("EXPENSIVE DB OPERATION: get_all_tables");
        self.0.get_all_tables()
    }

    /// Get the total count of records
    pub fn get_total_records(&mut self, table_name: &TableName) -> Result<usize, DbError> {
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

    pub fn get_users(&mut self) -> Result<Vec<User>, DbError> { self.0.get_users() }

    pub fn get_database_name(&mut self) -> Result<Option<DatabaseName>, DbError> {
        self.0.get_database_name()
    }

    /// get all table and views grouped per schema
    pub fn get_grouped_tables(&mut self) -> Result<Vec<SchemaContent>, DbError> {
        self.0.get_grouped_tables()
    }

    #[allow(unused_variables)]
    pub fn insert<T, R>(&mut self, entities: &[&T]) -> Result<Vec<R>, DbError>
    where
        T: ToTableName + ToColumnNames + ToDao,
        R: FromDao + ToColumnNames,
    {
        match self.0 {
            #[cfg(feature = "with-sqlite")]
            DBPlatform::Sqlite(_) => self.insert_simple(entities),
            #[cfg(feature = "with-postgres")]
            DBPlatform::Postgres(_) => self.insert_bulk_with_returning_support(entities),
            #[cfg(feature = "with-mysql")]
            DBPlatform::Mysql(_) => self.insert_simple(entities),
        }
    }

    /// called when the platform used is postgresql
    pub fn insert_bulk_with_returning_support<T, R>(
        &mut self,
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
    pub fn single_insert<T>(&mut self, entity: &T) -> Result<(), DbError>
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
    pub fn insert_simple<T, R>(&mut self, entities: &[&T]) -> Result<Vec<R>, DbError>
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
                        .map(|(x, _)| {
                            #[allow(unreachable_patterns)]
                            match self.0 {
                                #[cfg(feature = "with-sqlite")]
                                DBPlatform::Sqlite(_) => format!("${}", y * columns_len + x + 1),
                                #[cfg(feature = "with-postgres")]
                                DBPlatform::Postgres(_) => format!("${}", y * columns_len + x + 1),
                                #[cfg(feature = "with-mysql")]
                                DBPlatform::Mysql(_) => "?".to_string(),
                                _ => format!("${}", y * columns_len + x + 1),
                            }
                        })
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
        &mut self,
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
        &mut self,
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
        &mut self,
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
