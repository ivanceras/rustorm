# Unreleased
- Remove smarty algorithmn to cast blob image to data_uri, return as blob
- Add a function to check if a column is a primary to the table
- Implement setting and getting the autoincrement primary key of table for postgresql
- The ColumnConstraint AutoIncrement now contains the name of its corresponding sequence
- Add conversion of arrays to json
- simplify the default value in enum columns
- Implement displaying of text array
- Expose get_tablenames in EntityManager
- reexport uuid
- revise the SQL statement for getting the column default as it was dropped in postgresql 12

# 0.17.0
- Update rusqlite to 0.21
- Update r2d2_sqlite to 0.14
- Fix database pool being created every time a connection is requested.

# 0.16.0
 - Unify the interface for DatabaseMut + Database, EntityMut +Entity into their original name,
 - **Breaking change**: The query now requires the EntityManager to be passed as mutable.

# 0.15.4
    - use thiserror for implementing Error in rustorm_dao
    - rename sq module to a more appropriate sqlite since it does not conflict with the used crate name of sqlite which is rustqlite
# 0.15.3
    - implement FromValue for converting types that are not in the users' crate
    - remove panics on conversions
    - add supported parameter types
        - `Option<&'a str>`
        - `&Option<T>`
    - implement conversion of numeric to bool
    - add support ToDao, ToTableName, ToColumnNames to borrowed field contained struct

# 0.15.0
 - Mysql support
 - dao and codegen is not used as local path


# 0.14.0
 - Remove dependency to openssl
