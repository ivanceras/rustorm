use dao::Value;
use dao::value::Array;

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

impl SqlType {
    pub fn same_type(&self, value: &Value) -> bool {
        macro_rules! match_value{
            ($variant: ident) => {
                match *value {
                    Value::$variant(_) => true,
                    _ => false,
                }
            }
        }
        match *self {
            SqlType::Bool => match_value!(Bool),
            SqlType::Tinyint => match_value!(Tinyint),
            SqlType::Smallint => match_value!(Smallint),
            SqlType::Int => match_value!(Int),
            SqlType::Bigint => match_value!(Bigint),
            SqlType::Float => match_value!(Float),
            SqlType::Double =>  match_value!(Double),
            SqlType::Numeric => match_value!(BigDecimal),
            SqlType::Blob => match_value!(Blob),
            SqlType::Char => match_value!(Char),
            SqlType::Text 
                | SqlType::Varchar => match_value!(Text),
            SqlType::Json => match_value!(Json),
            SqlType::Uuid => match_value!(Uuid),
            SqlType::Date => match_value!(Date),
            SqlType::Timestamp => match_value!(Timestamp),
            SqlType::TimestampTz => match_value!(Timestamp),
            SqlType::Enum(_,_) => match_value!(Text),
            SqlType::ArrayType(ArrayType::Text) => match *value{
                Value::Array(Array::Text(_)) => true,
                _ => false
            }
            SqlType::ArrayType(ArrayType::Enum(_,_)) => match * value {
                Value::Array(Array::Text(_)) => true,
                _ => false
            }
            SqlType::Real => match *value{
                Value::Float(_) => true,
                _ => false
            }
            SqlType::ArrayType(ArrayType::Float) => match *value{
                Value::Array(Array::Float(_)) => true,
                _ => false
            }
            SqlType::TsVector => match_value!(Text),
            _ => panic!("not yet implemented for checking {:?} to {:?}", self, value),
        }
    }
}
