#![deny(warnings)]
#![deny(clippy::all)]
//!
//! ## Rustorm
//!
//! [![Latest Version](https://img.shields.io/crates/v/rustorm.svg)](https://crates.io/crates/rustorm)
//! [![Build Status](https://travis-ci.org/ivanceras/rustorm.svg?branch=master)](https://travis-ci.org/ivanceras/rustorm)
//! [![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
//!
//! Rustorm is an SQL-centered ORM with focus on ease of use on conversion of database types to
//! their appropriate rust type.
//!
//! Selecting records
//!
//! ```rust
//! use rustorm::{
//!     DbError,
//!     FromDao,
//!     Pool,
//!     ToColumnNames,
//!     ToTableName,
//! };
//!
//! #[derive(Debug, FromDao, ToColumnNames, ToTableName)]
//! struct Actor {
//!     actor_id: i32,
//!     first_name: String,
//! }
//!
//! #[cfg(any(feature="with-postgres", feature = "with-sqlite"))]
//! fn main() {
//!     let mut pool = Pool::new();
//!     #[cfg(feature = "with-sqlite")]
//!     let db_url = "sqlite://sakila.db";
//!     #[cfg(feature = "with-postgres")]
//!     let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
//!     let mut em = pool.em(db_url).unwrap();
//!     let sql = "SELECT * FROM actor LIMIT 10";
//!     let actors: Result<Vec<Actor>, DbError> =
//!         em.execute_sql_with_return(sql, &[]);
//!     println!("Actor: {:#?}", actors);
//!     let actors = actors.unwrap();
//!     assert_eq!(actors.len(), 10);
//!     for actor in actors {
//!         println!("actor: {:?}", actor);
//!     }
//! }
//! #[cfg(feature="with-mysql")]
//! fn main() {
//!    println!("see examples for mysql usage, mysql has a little difference in the api");
//! }
//! ```
//! Inserting and displaying the inserted records
//!
//! ```rust
//! use chrono::{
//!     offset::Utc,
//!     DateTime,
//!     NaiveDate,
//! };
//! use rustorm::{
//!     DbError,
//!     FromDao,
//!     Pool,
//!     TableName,
//!     ToColumnNames,
//!     ToDao,
//!     ToTableName,
//! };
//!
//!
//! #[cfg(any(feature="with-postgres", feature = "with-sqlite"))]
//! fn main() {
//!     mod for_insert {
//!         use super::*;
//!         #[derive(Debug, PartialEq, ToDao, ToColumnNames, ToTableName)]
//!         pub struct Actor {
//!             pub first_name: String,
//!             pub last_name: String,
//!         }
//!     }
//!
//!     mod for_retrieve {
//!         use super::*;
//!         #[derive(Debug, FromDao, ToColumnNames, ToTableName)]
//!         pub struct Actor {
//!             pub actor_id: i32,
//!             pub first_name: String,
//!             pub last_name: String,
//!             pub last_update: DateTime<Utc>,
//!         }
//!     }
//!
//!     let mut pool = Pool::new();
//!     #[cfg(feature = "with-sqlite")]
//!     let db_url = "sqlite://sakila.db";
//!     #[cfg(feature = "with-postgres")]
//!     let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
//!     let mut em = pool.em(db_url).unwrap();
//!     let tom_cruise = for_insert::Actor {
//!         first_name: "TOM".into(),
//!         last_name: "CRUISE".to_string(),
//!     };
//!     let tom_hanks = for_insert::Actor {
//!         first_name: "TOM".into(),
//!         last_name: "HANKS".to_string(),
//!     };
//!
//!     let actors: Result<Vec<for_retrieve::Actor>, DbError> =
//!         em.insert(&[&tom_cruise, &tom_hanks]);
//!     println!("Actor: {:#?}", actors);
//!     assert!(actors.is_ok());
//!     let actors = actors.unwrap();
//!     let today = Utc::now().date();
//!     assert_eq!(tom_cruise.first_name, actors[0].first_name);
//!     assert_eq!(tom_cruise.last_name, actors[0].last_name);
//!     assert_eq!(today, actors[0].last_update.date());
//!
//!     assert_eq!(tom_hanks.first_name, actors[1].first_name);
//!     assert_eq!(tom_hanks.last_name, actors[1].last_name);
//!     assert_eq!(today, actors[1].last_update.date());
//! }
//! #[cfg(feature="with-mysql")]
//! fn main() {
//!    println!("see examples for mysql usage, mysql has a little difference in the api");
//! }
//! ```
//! Rustorm is wholly used by [diwata](https://github.com/ivanceras/diwata)

use cfg_if::cfg_if;

cfg_if! {if #[cfg(feature = "with-postgres")]{
    extern crate r2d2_postgres;
    extern crate postgres;
    #[macro_use]
    extern crate postgres_shared;
    mod pg;
}}
cfg_if! {if #[cfg(feature = "with-sqlite")]{
    extern crate r2d2_sqlite;
    extern crate rusqlite;
    mod sqlite;
}}
cfg_if! {if #[cfg(feature = "with-mysql")]{
    mod my;
}}

pub mod column;
pub mod common;
mod dao_manager;
mod database;
mod entity;
pub mod error;
mod platform;
pub mod pool;
pub mod table;
pub mod types;
mod users;

pub mod util;

pub use column::Column;
pub use dao_manager::DaoManager;
pub use database::{
    Database,
    DatabaseName,
};
pub use entity::EntityManager;
pub use error::{
    DataError,
    DbError,
};
pub use platform::DBPlatform;
pub use pool::Pool;
pub use table::Table;

// we export the traits that has a derived proc macro
// this are used in the apps
pub use codegen::{
    FromDao,
    ToColumnNames,
    ToDao,
    ToTableName,
};

pub use rustorm_dao::{
    self,
    Array,
    ColumnName,
    ConvertError,
    Dao,
    FromValue,
    Rows,
    TableName,
    ToValue,
    Value,
};

/// Wrap the rustorm_dao exports to avoid name conflict with the rustorm_codegen
pub mod dao {
    pub use rustorm_dao::{
        FromDao,
        ToColumnNames,
        ToDao,
        ToTableName,
    };
}

/// Wrap the rustorm_codegen exports to avoid name conflict with the rustorm_dao
pub mod codegen {
    pub use rustorm_codegen::{
        FromDao,
        ToColumnNames,
        ToDao,
        ToTableName,
    };
}

#[macro_use]
extern crate log;
