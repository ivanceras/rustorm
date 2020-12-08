use rustorm::{DbError, FromDao, Pool, Rows, ToColumnNames, ToTableName};

fn main() {
    let db_url = "mysql://root:r00t@localhost/sakila";
    let mut pool = Pool::new();
    let mut dm = pool
        .dm(db_url)
        .expect("Should be able to get a connection here..");
    let sql = "SHOW TABLES";
    let rows: Result<Rows, DbError> = dm.execute_sql_with_return(sql, &[]);
    println!("rows: {:#?}", rows);
}
