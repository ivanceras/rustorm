use crate::{
    Rows,
    Table,
    TableName,
    Value,
};


use crate::DbError;




pub trait Database2 {
    fn execute_sql_with_return(&mut self, sql: &str, param: &[&Value]) -> Result<Rows, DbError>;

    fn get_table(&mut self, table_name: &TableName) -> Result<Table, DbError>;
}
