use chrono::{
    DateTime,
    Utc,
};
use rustorm_codegen::FromDao;
use serde::{
    Deserialize,
    Serialize,
};

#[allow(unused)]
mod previlege;

/// This is the user object mapped from pg_authid
#[derive(Debug, Serialize, Deserialize, FromDao)]
pub struct User {
    pub(crate) sysid: i32,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) is_superuser: bool,
    pub(crate) is_inherit: bool,
    pub(crate) can_create_db: bool,
    pub(crate) can_create_role: bool,
    pub(crate) can_login: bool,
    pub(crate) can_do_replication: bool,
    pub(crate) can_bypass_rls: bool,
    pub(crate) valid_until: Option<DateTime<Utc>>,
    pub(crate) conn_limit: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, FromDao)]
pub struct Role {
    pub role_name: String,
}
