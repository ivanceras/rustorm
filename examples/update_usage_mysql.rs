use rustorm::{
    DbError,
    FromDao,
    Pool,
    Rows,
    ToColumnNames,
    ToTableName,
};

/// Run using:
/// ```
/// cargo run --example update_usage_mysql --features "with-mysql"
/// ```

fn main() {
    let db_url = "mysql://root:r00tpwdh3r3@localhost/sakila";
    let mut pool = Pool::new();
    pool.ensure(db_url);
    let mut em = pool
        .em(db_url)
        .expect("Should be able to get a connection here..");
    let sql = "UPDATE actor SET last_name = ? WHERE first_name = ?".to_string();
    let rows: Result<Rows, DbError> = em
        .db()
        .execute_sql_with_return(&sql, &[&"JONES".into(), &"TOM".into()]);
    println!("rows: {:#?}", rows);
}
