use chrono::DateTime;
use chrono::Utc;
use rustorm_codegen::FromDao;
use serde_derive::{Deserialize, Serialize};

#[allow(unused)]
mod previlege;

/// This is the user object mapped from pg_authid
#[derive(Debug, Serialize, Deserialize, FromDao)]
pub struct User {
    sysid: i32,
    username: String,
    password: String,
    is_superuser: bool,
    is_inherit: bool,
    can_create_db: bool,
    can_create_role: bool,
    can_login: bool,
    can_do_replication: bool,
    can_bypass_rls: bool,
    valid_until: Option<DateTime<Utc>>,
    conn_limit: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, FromDao)]
pub struct Role {
    pub role_name: String,
}
