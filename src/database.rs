use dao::{Rows, Value};
use entity::EntityManager;
use table::Table;
use dao::TableName;

use error::DbError;

pub trait Database {

    fn execute_sql_with_return(&self, sql: &str, param: &[Value]) -> Result<Rows, DbError>;

    fn get_table(&self, em: &EntityManager, table_name: &TableName) -> Result<Table, DbError>;
}
