use dao::Value;

#[derive(Debug, Serialize, PartialEq, Clone)]
pub enum SqlType {
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
    TsVector,

    Uuid,
    Date,
    Timestamp,
    TimestampTz,

    Time,
    TimeTz,

    IpAddress,

    // enum list with the choices value
    Enum(String, Vec<String>),
    ArrayType(ArrayType),

    Custom(String),
}

#[derive(Debug, Serialize, PartialEq, Clone)]
pub enum ArrayType {
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

impl SqlType {
    pub fn same_type(&self, value: &Value) -> bool {
        match *self {
            SqlType::Bool => match *value {
                Value::Bool(_) => true,
                _ => false,
            },
            SqlType::Tinyint => match *value {
                Value::Tinyint(_) => true,
                _ => false,
            },
            SqlType::Smallint => match *value {
                Value::Smallint(_) => true,
                _ => false,
            },
            SqlType::Int => match *value {
                Value::Int(_) => true,
                _ => false,
            },
            SqlType::Bigint => match *value {
                Value::Bigint(_) => true,
                _ => false,
            },
            SqlType::Uuid => match *value {
                Value::Uuid(_) => true,
                _ => false,
            },
            _ => panic!("not yet implemented for checking {:?} to {:?}", self, value),
        }
    }
}
