use crate::{
    entity_mut::EntityManagerMut,
    table::SchemaContent,
    users::{
        Role,
        User,
    },
    DbError,
    Rows,
    Table,
    TableName,
    Value,
};
use rustorm_codegen::FromDao;
use serde::Serialize;


/// The current database name and its comment
#[derive(Serialize, FromDao)]
pub struct DatabaseName {
    name: String,
    description: Option<String>,
}



pub trait DatabaseMut {
    fn execute_sql_with_return(&mut self, sql: &str, param: &[&Value]) -> Result<Rows, DbError>;

    fn get_table(
        &mut self,
        em: &mut EntityManagerMut,
        table_name: &TableName,
    ) -> Result<Table, DbError>;

    fn get_all_tables(&mut self, em: &mut EntityManagerMut) -> Result<Vec<Table>, DbError>;

    fn get_grouped_tables(
        &mut self,
        em: &mut EntityManagerMut,
    ) -> Result<Vec<SchemaContent>, DbError>;

    fn get_users(&mut self, em: &mut EntityManagerMut) -> Result<Vec<User>, DbError>;

    fn get_roles(
        &mut self,
        em: &mut EntityManagerMut,
        username: &str,
    ) -> Result<Vec<Role>, DbError>;

    fn get_database_name(
        &mut self,
        em: &mut EntityManagerMut,
    ) -> Result<Option<DatabaseName>, DbError>;
}
