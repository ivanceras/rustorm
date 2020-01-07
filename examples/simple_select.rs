use rustorm::{
    DbError,
    Pool,
    Rows,
};

fn main() {
    let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
    let mut pool = Pool::new();
    let mut dm = pool.dm(db_url).unwrap();
    let sql = "SELECT * FROM actor LIMIT 10";
    let actors: Result<Rows, DbError> = dm.execute_sql_with_return(sql, &[]);
    println!("Actor: {:#?}", actors);
    let actors = actors.unwrap();
    assert_eq!(actors.iter().len(), 10);
    for actor in actors.iter() {
        println!("actor: {:?}", actor);
    }
}
