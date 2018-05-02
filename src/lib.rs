#![deny(warnings)]
#![allow(dead_code)]
#![feature(try_from)]
#![feature(splice)]
extern crate base64;
extern crate bigdecimal;
extern crate byteorder;
#[macro_use]
extern crate cfg_if;
extern crate chrono;
extern crate num_bigint;
extern crate num_integer;
extern crate num_traits;
extern crate r2d2;
#[macro_use]
extern crate rustorm_codegen;
extern crate rustorm_dao as dao;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate tree_magic;
extern crate url;
extern crate uuid;

cfg_if! {if #[cfg(feature = "with-postgres")]{
    extern crate r2d2_postgres;
    extern crate openssl;
    extern crate postgres;
    #[macro_use]
    extern crate postgres_shared;
    mod pg;
}}
cfg_if! {if #[cfg(feature = "with-sqlite")]{
    extern crate r2d2_sqlite3;
    extern crate sqlite3;
    mod sq;
}}

pub mod column;
pub mod common;
mod database;
mod entity;
pub mod error;
mod platform;
pub mod pool;
pub mod record;
pub mod record_manager;
pub mod table;
pub mod types;
mod util;

pub use column::Column;
pub use dao::ColumnName;
pub use dao::Rows;
pub use dao::TableName;
pub use dao::ToColumnNames;
pub use dao::ToTableName;
pub use dao::Value;
pub use dao::{FromDao, ToDao};
pub use database::Database;
pub use entity::EntityManager;
pub use error::DbError;
pub use pool::Pool;
pub use record::Record;
pub use record_manager::RecordManager;
pub use table::Table;

#[cfg(test)]
mod test {
    use super::*;
    use dao::{Dao, FromDao, ToDao};

    #[test]
    fn derive_fromdao_and_todao() {
        #[derive(Debug, PartialEq, FromDao, ToDao)]
        struct User {
            id: i32,
            username: String,
            active: Option<bool>,
        }

        let user = User {
            id: 1,
            username: "ivanceras".into(),
            active: Some(true),
        };
        println!("user: {:#?}", user);
        let dao = user.to_dao();
        let mut expected_dao = Dao::new();
        expected_dao.insert("id", 1);
        expected_dao.insert("username", "ivanceras".to_string());
        expected_dao.insert("active", true);

        assert_eq!(expected_dao, dao);

        println!("dao: {:#?}", dao);
        let from_dao = User::from_dao(&dao);
        println!("from_dao: {:#?}", from_dao);
        assert_eq!(from_dao, user);
    }
}
