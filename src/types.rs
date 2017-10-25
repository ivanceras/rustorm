

#[derive(Debug, PartialEq)]
pub enum SqlType{
    Bool,
    Tinyint,
    Smallint,
    Int,
    Bigint,

    SmallSerial,
    Serial,
    BigSerial,

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
    TextArray,
    NameArray,

    Uuid,
    Date,
    Timestamp,
    TimestampTz,

    // enum list with the choices value
    Enum(String, Vec<String>),

    Custom(String),
}
