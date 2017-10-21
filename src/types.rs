

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

    Uuid,
    Date,
    Timestamp,
    TimestampTz,

    Custom(String),
}
