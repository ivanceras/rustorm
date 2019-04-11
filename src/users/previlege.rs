use users::User;
use ColumnName;
use TableName;

/// User can have previlege to tables, to columns
/// The table models can be filtered depending on how much
///  and which columns it has privilege
enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Truncate,
    Connect,
    Execute,
}

///
///  CREATE TABLE user_privilege(
///     user_id int,
///     schema text,
///     table_name text,
///     columns text[], -- if no column mentioned, then the user has priviledge to all of the table columns
///     privilege text[],
///  )
/// User privileges for each tables
struct UserPrivilege {
    user: User,
    table_name: TableName,
    column_names: Vec<ColumnName>,
    privilege: Vec<Privilege>,
}
