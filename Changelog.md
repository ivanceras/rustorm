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
