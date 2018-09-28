use dao::value::Array;
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
            SqlType::Array(ref ty) => match ty.as_ref() {
                SqlType::Text => "text[]".into(),
                _ => panic!("not yet dealt {:?}", self),
            },
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

impl SqlType {
    pub fn same_type(&self, value: &Value) -> bool {
        macro_rules! match_value {
            ($variant:ident) => {
                match *value {
                    Value::$variant(_) => true,
                    _ => false,
                }
            };
        }
        match *self {
            SqlType::Bool => match_value!(Bool),
            SqlType::Tinyint => match_value!(Tinyint),
            SqlType::Smallint => match_value!(Smallint),
            SqlType::Int => match_value!(Int),
            SqlType::Bigint => match_value!(Bigint),
            SqlType::Float => match_value!(Float),
            SqlType::Double => match_value!(Double),
            SqlType::Numeric => match_value!(BigDecimal),
            SqlType::Blob => match_value!(Blob),
            SqlType::Char => match_value!(Char),
            SqlType::Text | SqlType::Varchar => match_value!(Text),
            SqlType::Json => match_value!(Json),
            SqlType::Point => match_value!(Point),
            SqlType::Uuid => match_value!(Uuid),
            SqlType::Date => match_value!(Date),
            SqlType::Timestamp => match_value!(Timestamp),
            SqlType::TimestampTz => match_value!(Timestamp),
            SqlType::Interval => match_value!(Interval),
            SqlType::Enum(_, _) => match_value!(Text),
            SqlType::Array(ref r) if SqlType::Text == *r.as_ref() => match *value {
                Value::Array(Array::Text(_)) => true,
                _ => false,
            },
            SqlType::Array(ref r) if SqlType::Int == *r.as_ref() => match *value {
                Value::Array(Array::Int(_)) => true,
                _ => false,
            },
            SqlType::Real => match *value {
                Value::Float(_) => true,
                _ => false,
            },
            SqlType::Array(ref r) if SqlType::Float == *r.as_ref() => match *value {
                Value::Array(Array::Float(_)) => true,
                _ => false,
            },
            SqlType::TsVector => match_value!(Text),
            _ => panic!("not yet implemented for checking {:?} to {:?}", self, value),
        }
    }
}
