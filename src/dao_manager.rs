use crate::{
    DBPlatform,
    Dao,
    DataError,
    DbError,
    Rows,
    Value,
};

pub struct DaoManager(pub DBPlatform);

impl DaoManager {
    pub fn execute_sql_with_return(
        &mut self,
        sql: &str,
        params: &[&Value],
    ) -> Result<Rows, DbError> {
        let rows = self.0.execute_sql_with_return(sql, params)?;
        Ok(rows)
    }

    pub fn execute_sql_with_records_return(
        &mut self,
        sql: &str,
        params: &[&Value],
    ) -> Result<Vec<Dao>, DbError> {
        let rows = self.0.execute_sql_with_return(sql, params)?;
        let daos: Vec<Dao> = rows.iter().collect();
        Ok(daos)
    }

    pub fn execute_sql_with_one_return(
        &mut self,
        sql: &str,
        params: &[&Value],
    ) -> Result<Dao, DbError> {
        let record: Result<Option<Dao>, DbError> =
            self.execute_sql_with_maybe_one_return(sql, params);
        match record {
            Ok(record) => {
                match record {
                    Some(record) => Ok(record),
                    None => Err(DbError::DataError(DataError::ZeroRecordReturned)),
                }
            }
            Err(e) => Err(e),
        }
    }

    pub fn execute_sql_with_maybe_one_return(
        &mut self,
        sql: &str,
        params: &[&Value],
    ) -> Result<Option<Dao>, DbError> {
        let result: Result<Vec<Dao>, DbError> = self.execute_sql_with_records_return(sql, params);
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
