use crate::{
    DatabaseMut,
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


pub enum DBPlatformMut {
    #[cfg(feature = "with-mysql")]
    Mysql(Box<MysqlDB>),
}

impl Deref for DBPlatformMut {
    type Target = dyn DatabaseMut;

    fn deref(&self) -> &Self::Target {
        match *self {
            #[cfg(feature = "with-mysql")]
            DBPlatformMut::Mysql(ref my) => my.deref(),
        }
    }
}

impl std::ops::DerefMut for DBPlatformMut {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            #[cfg(feature = "with-mysql")]
            DBPlatformMut::Mysql(ref mut my) => my.deref_mut(),
        }
    }
}
