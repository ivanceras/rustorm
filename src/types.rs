use rustorm_dao::{
    value::Array,
    Value,
};
use serde::{
    Deserialize,
    Serialize,
};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
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
    Interval,

    IpAddress,

    Point,

    // enum list with the choices value
    Enum(String, Vec<String>),
    Array(Box<SqlType>),
}

impl SqlType {
    pub fn is_array_type(&self) -> bool {
        match *self {
            SqlType::Array(_) => true,
            _ => false,
        }
    }

    pub fn is_integer_type(&self) -> bool {
        match *self {
            SqlType::Int => true,
            SqlType::Tinyint => true,
            SqlType::Smallint => true,
            SqlType::Bigint => true,
            _ => false,
        }
    }

    pub fn is_decimal_type(&self) -> bool {
        match *self {
            SqlType::Real => true,
            SqlType::Float => true,
            SqlType::Double => true,
            SqlType::Numeric => true,
            _ => false,
        }
    }

    pub fn cast_as(&self) -> Option<SqlType> {
        match *self {
            SqlType::TsVector => Some(SqlType::Text),
            _ => None,
        }
    }

    pub fn name(&self) -> String {
        match *self {
            SqlType::Text => "text".into(),
            SqlType::TsVector => "tsvector".into(),
            SqlType::Array(ref ty) => {
                match ty.as_ref() {
                    SqlType::Text => "text[]".into(),
                    _ => panic!("not yet dealt {:?}", self),
                }
            }
            _ => panic!("not yet dealt {:?}", self),
        }
    }
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

    Enum(String, Vec<String>),
}

trait HasType {
    fn get_type(&self) -> Option<SqlType>;
}

impl HasType for Value {
    fn get_type(&self) -> Option<SqlType> {
        match self {
            Value::Nil => None,
            Value::Bool(_) => Some(SqlType::Bool),
            Value::Tinyint(_) => Some(SqlType::Tinyint),
            Value::Smallint(_) => Some(SqlType::Smallint),
            Value::Int(_) => Some(SqlType::Int),
            Value::Bigint(_) => Some(SqlType::Bigint),
            Value::Float(_) => Some(SqlType::Float),
            Value::Double(_) => Some(SqlType::Double),
            Value::BigDecimal(_) => Some(SqlType::Numeric),
            Value::Blob(_) => Some(SqlType::Blob),
            Value::ImageUri(_) => Some(SqlType::Text),
            Value::Char(_) => Some(SqlType::Char),
            Value::Text(_) => Some(SqlType::Text),
            Value::Json(_) => Some(SqlType::Json),
            Value::Uuid(_) => Some(SqlType::Uuid),
            Value::Date(_) => Some(SqlType::Date),
            Value::Time(_) => Some(SqlType::Time),
            Value::DateTime(_) => Some(SqlType::Timestamp),
            Value::Timestamp(_) => Some(SqlType::Timestamp),
            Value::Interval(_) => Some(SqlType::Interval),
            Value::Point(_) => Some(SqlType::Point),
            Value::Array(Array::Int(_)) => {
                Some(SqlType::Array(Box::new(SqlType::Int)))
            }
            Value::Array(Array::Float(_)) => {
                Some(SqlType::Array(Box::new(SqlType::Float)))
            }
            Value::Array(Array::Text(_)) => {
                Some(SqlType::Array(Box::new(SqlType::Text)))
            }
        }
    }
}

impl SqlType {
    pub fn same_type(&self, value: &Value) -> bool {
        if let Some(simple_type) = value.get_type() {
            simple_type == *self
        } else {
            false
        }
    }
}
