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
#[cfg(feature = "db-auth")]
mod db_auth;
mod entity;
pub mod error;
mod platform;
pub mod pool;
pub mod table;
pub mod types;

pub mod util;

pub use chrono;
pub use column::ColumnDef;
pub use dao_manager::DaoManager;
pub use database::{Database, DatabaseName};
pub use entity::EntityManager;
pub use error::{DataError, DbError};
pub use platform::DBPlatform;
pub use pool::Pool;
pub use table::TableDef;
pub use uuid::{self, Uuid};

// we export the traits that has a derived proc macro
// this are used in the apps
pub use codegen::{FromDao, ToColumnNames, ToDao, ToTableName};

pub use rustorm_dao::{
    self, Array, ColumnName, ConvertError, Dao, FromValue, Rows, TableName, ToValue, Value,
};

/// Wrap the rustorm_dao exports to avoid name conflict with the rustorm_codegen
pub mod dao {
    pub use rustorm_dao::{FromDao, ToColumnNames, ToDao, ToTableName};
}

/// Wrap the rustorm_codegen exports to avoid name conflict with the rustorm_dao
pub mod codegen {
    pub use rustorm_codegen::{FromDao, ToColumnNames, ToDao, ToTableName};
}

#[macro_use]
extern crate log;
