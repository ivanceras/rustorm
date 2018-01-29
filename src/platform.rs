use url::Url;
use std::convert::TryFrom;
use error::ParseError;
use database::Database;
use std::ops::Deref;

cfg_if! {if #[cfg(feature = "with-postgres")]{
    use pg::PostgresDB;
}}

cfg_if! {if #[cfg(feature = "with-sqlite")]{
    use sq::SqliteDB;
}}

pub enum DBPlatform {
    #[cfg(feature = "with-postgres")] Postgres(PostgresDB),
    #[cfg(feature = "with-sqlite")] Sqlite(SqliteDB),
}

impl Deref for DBPlatform {
    type Target = Database;

    fn deref(&self) -> &Self::Target {
        match *self {
            #[cfg(feature = "with-postgres")]
            DBPlatform::Postgres(ref pg) => pg,
            #[cfg(feature = "with-sqlite")]
            DBPlatform::Sqlite(ref sq) => sq,
        }
    }
}

pub(crate) enum Platform {
    #[cfg(feature = "with-postgres")] Postgres,
    #[cfg(feature = "with-sqlite")] Sqlite(String),
    Unsupported(String),
}

impl<'a> TryFrom<&'a str> for Platform {
    type Error = ParseError;

    fn try_from(s: &'a str) -> Result<Self, Self::Error> {
        let url = Url::parse(s);
        match url {
            Ok(url) => {
                println!("url: {:#?}", url);
                println!("host: {:?}", url.host_str());
                let scheme = url.scheme();
                match scheme {
                    #[cfg(feature = "with-postgres")]
                    "postgres" => Ok(Platform::Postgres),
                    #[cfg(feature = "with-sqlite")]
                    "sqlite" => Ok(Platform::Sqlite(url.host_str().unwrap().to_owned())),
                    _ => Ok(Platform::Unsupported(scheme.to_string())),
                }
            }
            Err(e) => Err(ParseError::DbUrlParseError(e)),
        }
    }
}
