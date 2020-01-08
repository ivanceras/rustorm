use crate::{
    error::ParseError,
    Database,
};
use cfg_if::cfg_if;
use std::{
    convert::TryFrom,
    ops::Deref,
};
use url::Url;


cfg_if! {if #[cfg(feature = "with-postgres")]{
    use crate::pg::PostgresDB;
}}

cfg_if! {if #[cfg(feature = "with-sqlite")]{
    use crate::sqlite::SqliteDB;
}}

cfg_if! {if #[cfg(feature = "with-mysql")]{
    use crate::my::MysqlDB;
}}


pub enum DBPlatform {
    #[cfg(feature = "with-postgres")]
    Postgres(Box<PostgresDB>),
    #[cfg(feature = "with-sqlite")]
    Sqlite(Box<SqliteDB>),
    #[cfg(feature = "with-mysql")]
    Mysql(Box<MysqlDB>),
}

impl Deref for DBPlatform {
    type Target = dyn Database;

    fn deref(&self) -> &Self::Target {
        match *self {
            #[cfg(feature = "with-postgres")]
            DBPlatform::Postgres(ref pg) => pg.deref(),
            #[cfg(feature = "with-sqlite")]
            DBPlatform::Sqlite(ref sq) => sq.deref(),
            #[cfg(feature = "with-mysql")]
            DBPlatform::Mysql(ref my) => my.deref(),
        }
    }
}

impl std::ops::DerefMut for DBPlatform {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match *self {
            #[cfg(feature = "with-postgres")]
            DBPlatform::Postgres(ref mut pg) => pg.deref_mut(),
            #[cfg(feature = "with-sqlite")]
            DBPlatform::Sqlite(ref mut sq) => sq.deref_mut(),
            #[cfg(feature = "with-mysql")]
            DBPlatform::Mysql(ref mut my) => my.deref_mut(),
        }
    }
}

pub(crate) enum Platform {
    #[cfg(feature = "with-postgres")]
    Postgres,
    #[cfg(feature = "with-sqlite")]
    Sqlite(String),
    #[cfg(feature = "with-mysql")]
    Mysql,
    Unsupported(String),
}

impl<'a> TryFrom<&'a str> for Platform {
    type Error = ParseError;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        let url = Url::parse(s);
        match url {
            Ok(url) => {
                info!("url: {:#?}", url);
                info!("host: {:?}", url.host_str());
                info!("path: {:?}", url.path());
                let scheme = url.scheme();
                match scheme {
                    #[cfg(feature = "with-postgres")]
                    "postgres" => Ok(Platform::Postgres),
                    #[cfg(feature = "with-sqlite")]
                    "sqlite" => {
                        let host = url.host_str().unwrap();
                        let path = url.path();
                        let path = if path == "/" { "" } else { path };
                        let db_file = format!("{}{}", host, path);
                        Ok(Platform::Sqlite(db_file))
                    }
                    #[cfg(feature = "with-mysql")]
                    "mysql" => Ok(Platform::Mysql),
                    _ => Ok(Platform::Unsupported(scheme.to_string())),
                }
            }
            Err(e) => Err(ParseError::DbUrlParseError(e)),
        }
    }
}
