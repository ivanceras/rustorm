//! 
//! Selecting records
//!
//! ```rust
//! #[macro_use]
//! extern crate rustorm_codegen;
//! extern crate rustorm_dao as dao;
//! extern crate rustorm_dao;
//! extern crate rustorm;
//! use rustorm::TableName;
//! use rustorm_dao::ToColumnNames;
//! use rustorm_dao::ToTableName;
//! use rustorm_dao::{FromDao, ToDao};
//! use rustorm::Pool;
//! use rustorm::DbError;
//! 
//! #[derive(Debug, FromDao, ToColumnNames, ToTableName)]
//! struct Actor {
//!     actor_id: i32,
//!     first_name: String,
//! }
//! 
//! fn main(){
//!     let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
//!     let mut pool = Pool::new();
//!     let em = pool.em(db_url).unwrap();
//!     let sql = "SELECT * FROM actor LIMIT 10";
//!     let actors: Result<Vec<Actor>, DbError> = em.execute_sql_with_return(sql, &[]);
//!     println!("Actor: {:#?}", actors);
//!     let actors = actors.unwrap();
//!     assert_eq!(actors.len(), 10);
//!     for actor in actors {
//!         println!("actor: {:?}", actor);
//!     }
//! }
//! ```
//! Inserting and displaying the inserted records
//!
//! ```rust
//! #[macro_use]
//! extern crate rustorm_codegen;
//! extern crate rustorm_dao as dao;
//! extern crate rustorm_dao;
//! extern crate rustorm;
//! extern crate chrono;
//!
//! use rustorm::TableName;
//! use rustorm_dao::ToColumnNames;
//! use rustorm_dao::ToTableName;
//! use rustorm_dao::{FromDao, ToDao};
//! use rustorm::Pool;
//! use rustorm::DbError;
//! use chrono::offset::Utc;
//! use chrono::{DateTime, NaiveDate};
//!
//!   fn main() {
//!       mod for_insert {
//!           use super::*;
//!           #[derive(Debug, PartialEq, ToDao, ToColumnNames, ToTableName)]
//!           pub struct Actor {
//!               pub first_name: String,
//!               pub last_name: String,
//!           }
//!       }
//!
//!       mod for_retrieve {
//!           use super::*;
//!           #[derive(Debug, FromDao, ToColumnNames, ToTableName)]
//!           pub struct Actor {
//!               pub actor_id: i32,
//!               pub first_name: String,
//!               pub last_name: String,
//!               pub last_update: DateTime<Utc>,
//!           }
//!       }
//!
//!       let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
//!       let mut pool = Pool::new();
//!       let em = pool.em(db_url).unwrap();
//!       let tom_cruise = for_insert::Actor {
//!           first_name: "TOM".into(),
//!           last_name: "CRUISE".to_string(),
//!       };
//!       let tom_hanks = for_insert::Actor {
//!           first_name: "TOM".into(),
//!           last_name: "HANKS".to_string(),
//!       };
//!
//!       let actors: Result<Vec<for_retrieve::Actor>, DbError> =
//!           em.insert(&[&tom_cruise, &tom_hanks]);
//!       println!("Actor: {:#?}", actors);
//!       assert!(actors.is_ok());
//!       let actors = actors.unwrap();
//!       let today = Utc::now().date();
//!       assert_eq!(tom_cruise.first_name, actors[0].first_name);
//!       assert_eq!(tom_cruise.last_name, actors[0].last_name);
//!       assert_eq!(today, actors[0].last_update.date());
//!       assert_eq!(tom_hanks.first_name, actors[1].first_name);
//!       assert_eq!(tom_hanks.last_name, actors[1].last_name);
//!       assert_eq!(today, actors[1].last_update.date());
//!   }
//! ```
//!
#![feature(external_doc)]
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
extern crate geo;
extern crate serde_json;
extern crate time;
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
mod users;

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
pub use database::DatabaseName;

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
