use cfg_if::cfg_if;
use log::*;
#[cfg(any(feature = "with-postgres", feature = "with-sqlite"))]
use r2d2;

cfg_if! {if #[cfg(feature = "with-postgres")]{
    use r2d2_postgres::PostgresConnectionManager;
    use crate::pg::{self, PostgresDB};
}}
cfg_if! {if #[cfg(feature = "with-sqlite")]{
    use r2d2_sqlite::SqliteConnectionManager;
    use crate::sqlite::{self, SqliteDB};
}}
cfg_if! {if #[cfg(feature = "with-mysql")]{
    use r2d2_mysql::MysqlConnectionManager;
    use crate::my::{self, MysqlDB};
}}

use crate::{
    error::{
        ConnectError,
        ParseError,
    },
    platform::Platform,
    DBPlatform,
    DaoManager,
    DbError,
    EntityManager,
};
use std::{
    collections::BTreeMap,
    convert::TryFrom,
};

#[derive(Default)]
pub struct Pool(BTreeMap<String, ConnPool>);
pub enum ConnPool {
    #[cfg(feature = "with-postgres")]
    PoolPg(r2d2::Pool<PostgresConnectionManager>),
    #[cfg(feature = "with-sqlite")]
    PoolSq(r2d2::Pool<SqliteConnectionManager>),
    #[cfg(feature = "with-mysql")]
    PoolMy(r2d2::Pool<MysqlConnectionManager>),
}

pub enum PooledConn {
    #[cfg(feature = "with-postgres")]
    PooledPg(Box<r2d2::PooledConnection<PostgresConnectionManager>>),
    #[cfg(feature = "with-sqlite")]
    PooledSq(Box<r2d2::PooledConnection<SqliteConnectionManager>>),
    #[cfg(feature = "with-mysql")]
    PooledMy(Box<r2d2::PooledConnection<MysqlConnectionManager>>),
}

impl Pool {
    pub fn new() -> Self { Default::default() }

    /// ensure that a connection pool for this db_url exist
    pub fn ensure(&mut self, db_url: &str) -> Result<(), DbError> {
        info!("ensure db_url: {}", db_url);
        let platform: Result<Platform, _> = TryFrom::try_from(db_url);
        match platform {
            Ok(platform) => {
                match platform {
                    #[cfg(feature = "with-postgres")]
                    Platform::Postgres => {
                        let pool_pg = pg::init_pool(db_url)?;
                        if self.0.get(db_url).is_none() {
                            self.0.insert(db_url.to_string(), ConnPool::PoolPg(pool_pg));
                        }
                        Ok(())
                    }
                    #[cfg(feature = "with-sqlite")]
                    Platform::Sqlite(path) => {
                        info!("matched sqlite");
                        let pool_sq = sqlite::init_pool(&path)?;
                        if self.0.get(db_url).is_none() {
                            self.0.insert(db_url.to_string(), ConnPool::PoolSq(pool_sq));
                        }
                        Ok(())
                    }
                    #[cfg(feature = "with-mysql")]
                    Platform::Mysql => {
                        let pool_my = my::init_pool(db_url)?;
                        if self.0.get(db_url).is_none() {
                            self.0.insert(db_url.to_string(), ConnPool::PoolMy(pool_my));
                        }
                        Ok(())
                    }
                    Platform::Unsupported(scheme) => {
                        info!("unsupported");
                        Err(DbError::ConnectError(ConnectError::UnsupportedDb(scheme)))
                    }
                }
            }
            Err(e) => Err(DbError::ConnectError(ConnectError::ParseError(e))),
        }
    }

    /// get the pool for this specific db_url, create one if it doesn't have yet.
    fn get_pool(&mut self, db_url: &str) -> Result<&ConnPool, DbError> {
        self.ensure(db_url)?;
        let platform: Result<Platform, ParseError> = TryFrom::try_from(db_url);
        match platform {
            Ok(platform) => {
                match platform {
                    #[cfg(feature = "with-postgres")]
                    Platform::Postgres => {
                        let conn: Option<&ConnPool> = self.0.get(db_url);
                        if let Some(conn) = conn {
                            Ok(conn)
                        } else {
                            Err(DbError::ConnectError(ConnectError::NoSuchPoolConnection))
                        }
                    }
                    #[cfg(feature = "with-sqlite")]
                    Platform::Sqlite(_path) => {
                        info!("getting sqlite pool");
                        let conn: Option<&ConnPool> = self.0.get(db_url);
                        if let Some(conn) = conn {
                            Ok(conn)
                        } else {
                            Err(DbError::ConnectError(ConnectError::NoSuchPoolConnection))
                        }
                    }
                    #[cfg(feature = "with-mysql")]
                    Platform::Mysql => {
                        let conn: Option<&ConnPool> = self.0.get(db_url);
                        if let Some(conn) = conn {
                            Ok(conn)
                        } else {
                            Err(DbError::ConnectError(ConnectError::NoSuchPoolConnection))
                        }
                    }

                    Platform::Unsupported(scheme) => {
                        Err(DbError::ConnectError(ConnectError::UnsupportedDb(scheme)))
                    }
                }
            }
            Err(e) => Err(DbError::ConnectError(ConnectError::ParseError(e))),
        }
    }

    /// get a usable database connection from
    pub fn connect(&mut self, db_url: &str) -> Result<PooledConn, DbError> {
        let pool = self.get_pool(db_url)?;
        match *pool {
            #[cfg(feature = "with-postgres")]
            ConnPool::PoolPg(ref pool_pg) => {
                let pooled_conn = pool_pg.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConn::PooledPg(Box::new(pooled_conn))),
                    Err(e) => Err(DbError::ConnectError(ConnectError::R2d2Error(e))),
                }
            }
            #[cfg(feature = "with-sqlite")]
            ConnPool::PoolSq(ref pool_sq) => {
                let pooled_conn = pool_sq.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConn::PooledSq(Box::new(pooled_conn))),
                    Err(e) => Err(DbError::ConnectError(ConnectError::R2d2Error(e))),
                }
            }
            #[cfg(feature = "with-mysql")]
            ConnPool::PoolMy(ref pool_my) => {
                let pooled_conn = pool_my.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConn::PooledMy(Box::new(pooled_conn))),
                    Err(e) => Err(DbError::ConnectError(ConnectError::R2d2Error(e))),
                }
            }
        }
    }

    pub fn dm(&mut self, db_url: &str) -> Result<DaoManager, DbError> {
        let db = self.db(db_url)?;
        Ok(DaoManager(db))
    }

    /// get the pool for this specific db_url, create one if it doesn't have yet.
    fn get_pool_mut(&mut self, db_url: &str) -> Result<&ConnPool, DbError> {
        self.ensure(db_url)?;
        let platform: Result<Platform, ParseError> = TryFrom::try_from(db_url);
        match platform {
            Ok(platform) => {
                match platform {
                    #[cfg(feature = "with-postgres")]
                    Platform::Postgres => {
                        let conn: Option<&ConnPool> = self.0.get(db_url);
                        if let Some(conn) = conn {
                            Ok(conn)
                        } else {
                            Err(DbError::ConnectError(ConnectError::NoSuchPoolConnection))
                        }
                    }
                    #[cfg(feature = "with-sqlite")]
                    Platform::Sqlite(_path) => {
                        info!("getting sqlite pool");
                        let conn: Option<&ConnPool> = self.0.get(db_url);
                        if let Some(conn) = conn {
                            Ok(conn)
                        } else {
                            Err(DbError::ConnectError(ConnectError::NoSuchPoolConnection))
                        }
                    }
                    #[cfg(feature = "with-mysql")]
                    Platform::Mysql => {
                        let conn: Option<&ConnPool> = self.0.get(db_url);
                        if let Some(conn) = conn {
                            Ok(conn)
                        } else {
                            Err(DbError::ConnectError(ConnectError::NoSuchPoolConnection))
                        }
                    }

                    Platform::Unsupported(scheme) => {
                        Err(DbError::ConnectError(ConnectError::UnsupportedDb(scheme)))
                    }
                }
            }
            Err(e) => Err(DbError::ConnectError(ConnectError::ParseError(e))),
        }
    }

    /// get a usable database connection from
    pub fn connect_mut(&mut self, db_url: &str) -> Result<PooledConn, DbError> {
        let pool = self.get_pool_mut(db_url)?;
        match *pool {
            #[cfg(feature = "with-postgres")]
            ConnPool::PoolPg(ref pool_pg) => {
                let pooled_conn = pool_pg.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConn::PooledPg(Box::new(pooled_conn))),
                    Err(e) => Err(DbError::ConnectError(ConnectError::R2d2Error(e))),
                }
            }
            #[cfg(feature = "with-sqlite")]
            ConnPool::PoolSq(ref pool_sq) => {
                let pooled_conn = pool_sq.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConn::PooledSq(Box::new(pooled_conn))),
                    Err(e) => Err(DbError::ConnectError(ConnectError::R2d2Error(e))),
                }
            }
            #[cfg(feature = "with-mysql")]
            ConnPool::PoolMy(ref pool_my) => {
                let pooled_conn = pool_my.get();
                match pooled_conn {
                    Ok(pooled_conn) => Ok(PooledConn::PooledMy(Box::new(pooled_conn))),
                    Err(e) => Err(DbError::ConnectError(ConnectError::R2d2Error(e))),
                }
            }
        }
    }

    /// get a database instance with a connection, ready to send sql statements
    pub fn db(&mut self, db_url: &str) -> Result<DBPlatform, DbError> {
        let pooled_conn = self.connect_mut(db_url)?;

        match pooled_conn {
            #[cfg(feature = "with-postgres")]
            PooledConn::PooledPg(pooled_pg) => {
                Ok(DBPlatform::Postgres(Box::new(PostgresDB(*pooled_pg))))
            }
            #[cfg(feature = "with-sqlite")]
            PooledConn::PooledSq(pooled_sq) => {
                Ok(DBPlatform::Sqlite(Box::new(SqliteDB(*pooled_sq))))
            }
            #[cfg(feature = "with-mysql")]
            PooledConn::PooledMy(pooled_my) => Ok(DBPlatform::Mysql(Box::new(MysqlDB(*pooled_my)))),
        }
    }

    pub fn em(&mut self, db_url: &str) -> Result<EntityManager, DbError> {
        let db = self.db(db_url)?;
        Ok(EntityManager(db))
    }
}

pub fn test_connection(db_url: &str) -> Result<(), DbError> {
    let platform: Result<Platform, ParseError> = TryFrom::try_from(db_url);
    match platform {
        Ok(platform) => {
            match platform {
                #[cfg(feature = "with-postgres")]
                Platform::Postgres => {
                    pg::test_connection(db_url)?;
                    Ok(())
                }
                #[cfg(feature = "with-sqlite")]
                Platform::Sqlite(path) => {
                    info!("testing connection: {}", path);
                    sqlite::test_connection(&path)?;
                    Ok(())
                }
                #[cfg(feature = "with-mysql")]
                Platform::Mysql => {
                    my::test_connection(db_url)?;
                    Ok(())
                }
                Platform::Unsupported(scheme) => {
                    Err(DbError::ConnectError(ConnectError::UnsupportedDb(scheme)))
                }
            }
        }
        Err(e) => Err(DbError::ConnectError(ConnectError::ParseError(e))),
    }
}

#[cfg(test)]
#[cfg(feature = "with-postgres")]
mod tests_pg {
    use super::*;

    #[test]
    fn connect() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        pool.ensure(db_url).expect("Unable to initialize pool");
        let pooled = pool.get_pool(db_url);
        match pooled {
            Ok(_) => info!("ok"),
            Err(ref e) => info!("error: {:?}", e),
        }
        assert!(pooled.is_ok());
    }

    #[test]
    fn connect_no_ensure() {
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        assert!(pool.get_pool(db_url).is_ok());
    }
}
