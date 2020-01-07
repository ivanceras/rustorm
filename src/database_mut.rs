use crate::{
    database::DatabaseName,
    entity_mut::EntityManagerMut,
    table::SchemaContent,
    users::{
        Role,
        User,
    },
    Rows,
    Table,
    TableName,
    Value,
};


use crate::DbError;



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
