#![deny(warnings)]

use rustorm::{
    FromDao,
    ToDao,
    ToTableName,
};

#[derive(Debug, FromDao, ToDao, ToTableName)]
struct User {
    id: i32,
    username: String,
}

fn main() {
    // imported here since we are using the trait methods
    // `to_dao` and `to_table_name` without
    // conflicting with the derive ToDao and ToTableName macro
    use rustorm::dao::{
        ToDao,
        ToTableName,
    };

    let user = User {
        id: 1,
        username: "ivanceras".to_string(),
    };
    println!("user: {:#?}", user);
    let dao = user.to_dao();
    println!("dao: {:#?}", dao);
    let table = User::to_table_name();
    println!("table name: {}", table.name);
    println!("table: {:#?}", table);
}
