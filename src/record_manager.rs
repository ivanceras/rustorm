use record::Record;
use platform::DBPlatform;
use error::DbError;
use dao::Value;
use error::DataError;
use dao::Rows;

pub struct RecordManager(pub DBPlatform);

impl RecordManager {
    pub fn execute_sql_with_return(&self, sql: &str, params: &[Value]) -> Result<Rows, DbError> {
        let rows = self.0.execute_sql_with_return(sql, params)?;
        Ok(rows)
    }

    pub fn execute_sql_with_records_return(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Vec<Record>, DbError> {
        let rows = self.0.execute_sql_with_return(sql, params)?;
        Ok(rows.iter()
            .map(|dao| Record::from(&dao))
            .collect::<Vec<Record>>())
    }

    pub fn execute_sql_with_one_return(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Record, DbError> {
        let record: Result<Option<Record>, DbError> =
            self.execute_sql_with_maybe_one_return(sql, params);
        match record {
            Ok(record) => match record {
                Some(record) => Ok(record),
                None => Err(DbError::DataError(DataError::ZeroRecordReturned)),
            },
            Err(e) => Err(e),
        }
    }

    pub fn execute_sql_with_maybe_one_return(
        &self,
        sql: &str,
        params: &[Value],
    ) -> Result<Option<Record>, DbError> {
        let result: Result<Vec<Record>, DbError> =
            self.execute_sql_with_records_return(sql, params);
        match result {
            Ok(mut result) => match result.len() {
                0 => Ok(None),
                1 => Ok(Some(result.remove(0))),
                _ => Err(DbError::DataError(DataError::MoreThan1RecordReturned)),
            },
            Err(e) => Err(e),
        }
    }
}
