#[macro_use]
extern crate rustorm_codegen;
extern crate rustorm;
extern crate rustorm_dao;
extern crate rustorm_dao as dao;
use rustorm::DbError;
use rustorm::Pool;
use rustorm::Rows;
use rustorm::TableName;
use rustorm_dao::ToColumnNames;
use rustorm_dao::ToTableName;
use rustorm_dao::{FromDao, ToDao};

fn main() {
    let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
    let mut pool = Pool::new();
    let dm = pool.dm(db_url).unwrap();
    let sql = "SELECT * FROM actor LIMIT 10";
    let actors: Result<Rows, DbError> = dm.execute_sql_with_return(sql, &[]);
    println!("Actor: {:#?}", actors);
    let actors = actors.unwrap();
    assert_eq!(actors.iter().len(), 10);
    for actor in actors.iter() {
        println!("actor: {:?}", actor);
    }
}
