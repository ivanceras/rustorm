use crate::{
    Database2,
};
use cfg_if::cfg_if;

use std::{
    ops::Deref,
};


cfg_if! {if #[cfg(feature = "with-postgres")]{
    use crate::pg::PostgresDB;
}}

cfg_if! {if #[cfg(feature = "with-sqlite")]{
    use crate::sq::SqliteDB;
}}

cfg_if! {if #[cfg(feature = "with-mysql")]{
    use crate::my::MysqlDB;
}}


pub enum DBPlatform2 {
    #[cfg(feature = "with-mysql")]
    Mysql(Box<MysqlDB>),
}

impl Deref for DBPlatform2 {
    type Target = dyn Database2;

    fn deref(&self) -> &Self::Target {
        match *self {
            #[cfg(feature = "with-mysql")]
            DBPlatform2::Mysql(ref my) => my.deref(),
        }
    }
}

impl std::ops::DerefMut for DBPlatform2 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            #[cfg(feature = "with-mysql")]
            DBPlatform2::Mysql(ref mut my) => my.deref_mut(),
        }
    }
}
