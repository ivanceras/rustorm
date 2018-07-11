#[macro_use]
extern crate rustorm_codegen;
extern crate rustorm_dao as dao;
extern crate rustorm_dao;
extern crate rustorm;
use rustorm::TableName;
use rustorm_dao::ToColumnNames;
use rustorm_dao::ToTableName;
use rustorm_dao::{FromDao, ToDao};
use rustorm::Pool;
use rustorm::DbError;

#[derive(Debug, FromDao, ToColumnNames, ToTableName)]
struct Actor {
    actor_id: i32,
    first_name: String,
}

fn main(){
    let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
    let mut pool = Pool::new();
    let em = pool.em(db_url).unwrap();
    let sql = "SELECT * FROM actor LIMIT 10";
    let actors: Result<Vec<Actor>, DbError> = em.execute_sql_with_return(sql, &[]);
    println!("Actor: {:#?}", actors);
    let actors = actors.unwrap();
    assert_eq!(actors.len(), 10);
    for actor in actors {
        println!("actor: {:?}", actor);
    }
}
