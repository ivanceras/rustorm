#[cfg(feature = "db-auth")]
use crate::db_auth::{Role, User};
use crate::{table::SchemaContent, DbError, Rows, TableDef, TableName, Value};
use rustorm_codegen::FromDao;
use serde::Serialize;

/// The current database name and its comment
#[derive(Serialize, FromDao)]
pub struct DatabaseName {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
}

pub trait Database {
    fn begin_transaction(&mut self) -> Result<(), DbError>;

    fn commit_transaction(&mut self) -> Result<(), DbError>;

    fn rollback_transaction(&mut self) -> Result<(), DbError>;

    fn execute_sql_with_return(&mut self, sql: &str, param: &[&Value]) -> Result<Rows, DbError>;

    fn get_table(&mut self, table_name: &TableName) -> Result<Option<TableDef>, DbError>;

    fn set_autoincrement_value(
        &mut self,
        table_name: &TableName,
        sequence_value: i64,
    ) -> Result<Option<i64>, DbError>;

    fn get_autoincrement_last_value(
        &mut self,
        table_name: &TableName,
    ) -> Result<Option<i64>, DbError>;

    fn get_all_tables(&mut self) -> Result<Vec<TableDef>, DbError>;

    fn get_tablenames(&mut self) -> Result<Vec<TableName>, DbError>;

    fn get_grouped_tables(&mut self) -> Result<Vec<SchemaContent>, DbError>;

    fn get_database_name(&mut self) -> Result<Option<DatabaseName>, DbError>;

    #[cfg(feature = "db-auth")]
    fn get_users(&mut self) -> Result<Vec<User>, DbError>;

    #[cfg(feature = "db-auth")]
    fn get_user_detail(&mut self, username: &str) -> Result<Vec<User>, DbError>;

    #[cfg(feature = "db-auth")]
    fn get_roles(&mut self, username: &str) -> Result<Vec<Role>, DbError>;
}
