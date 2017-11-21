

#[derive(Debug, Serialize, PartialEq, Clone)]
pub enum SqlType{
    Bool,
    Tinyint,
    Smallint,
    Int,
    Bigint,

    Real,
    Float,
    Double,
    Numeric,

    Tinyblob,
    Mediumblob, 
    Blob,
    Longblob,
    Varbinary,

    Char,
    Varchar,
    Tinytext,
    Mediumtext,
    Text,
    Json,

    Uuid,
    Date,
    Timestamp,
    TimestampTz,

    Time,
    TimeTz,

    // enum list with the choices value
    Enum(String, Vec<String>),
    ArrayType(ArrayType),

    Custom(String),
}


#[derive(Debug, Serialize, PartialEq, Clone)]
pub enum ArrayType{
    Bool,
    Tinyint,
    Smallint,
    Int,
    Bigint,

    Real,
    Float,
    Double,
    Numeric,

    Char,
    Varchar,
    Tinytext,
    Mediumtext,
    Text,

    Uuid,
    Date,
    Timestamp,
    TimestampTz,

}
