use chrono::{DateTime, Utc};
use rustorm_codegen::FromDao;
use serde::{Deserialize, Serialize};

#[allow(unused)]
mod previlege;

/// This is the user object mapped from pg_authid
#[derive(Debug, Serialize, Deserialize, FromDao)]
pub struct User {
    pub sysid: i32,
    pub username: String,
    pub is_superuser: bool,
    pub is_inherit: bool,
    pub can_create_db: bool,
    pub can_create_role: bool,
    pub can_login: bool,
    pub can_do_replication: bool,
    pub can_bypass_rls: bool,
    pub valid_until: Option<DateTime<Utc>>,
    pub conn_limit: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, FromDao)]
pub struct Role {
    pub role_name: String,
}
