#![allow(clippy::cast_lossless)]
use crate::{
    interval::Interval,
    ConvertError,
};
use bigdecimal::{
    BigDecimal,
    ToPrimitive,
};
use chrono::{
    DateTime,
    NaiveDate,
    NaiveDateTime,
    NaiveTime,
    Utc,
};
use geo::Point;
use serde_derive::{
    Deserialize,
    Serialize,
};
use std::fmt;
use uuid::Uuid;

/// Generic value storage 32 byte in size
/// Some contains the same value container, but the variant is more
/// important for type hinting and view presentation hinting purposes
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Value {
    Nil, // no value
    Bool(bool),

    Tinyint(i8),
    Smallint(i16),
    Int(i32),
    Bigint(i64),

    Float(f32),
    Double(f64),
    BigDecimal(BigDecimal),

    Blob(Vec<u8>),
    ImageUri(String),
    Char(char),
    Text(String),
    Json(String),

    Uuid(Uuid),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(NaiveDateTime),
    Timestamp(DateTime<Utc>),
    Interval(Interval),

    Point(Point<f64>),

    Array(Array),
}

impl Value {
    pub fn is_nil(&self) -> bool { *self == Value::Nil }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::Nil => write!(f, ""),
            Value::Bool(v) => write!(f, "{}", v),
            Value::Tinyint(v) => write!(f, "{}", v),
            Value::Smallint(v) => write!(f, "{}", v),
            Value::Int(v) => write!(f, "{}", v),
            Value::Bigint(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::Double(v) => write!(f, "{}", v),
            Value::BigDecimal(v) => write!(f, "{}", v),
            Value::ImageUri(v) => write!(f, "{}", v),
            Value::Char(v) => write!(f, "{}", v),
            Value::Text(v) => write!(f, "{}", v),
            Value::Json(v) => write!(f, "{}", v),
            Value::Uuid(v) => write!(f, "{}", v),
            Value::Date(v) => write!(f, "{}", v),
            Value::Time(v) => write!(f, "{}", v),
            Value::DateTime(v) => write!(f, "{}", v.format("%Y-%m-%d %H:%M:%S").to_string()),
            Value::Timestamp(v) => write!(f, "{}", v.to_rfc3339()),
            _ => todo!(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Array {
    /*
    Bool(Vec<bool>),

    Tinyint(Vec<i8>),
    Smallint(Vec<i16>),
    */
    Int(Vec<i32>),
    Float(Vec<f32>),
    /*
    Bigint(Vec<i64>),

    Double(Vec<f64>),
    BigDecimal(Vec<BigDecimal>),
    */
    Text(Vec<String>),
    /*
    Char(Vec<char>),
    Uuid(Vec<Uuid>),
    Date(Vec<NaiveDate>),
    Timestamp(Vec<DateTime<Utc>>),
    */
}

/// A trait to allow passing of parameters ergonomically
/// in em.execute_sql_with_return
pub trait ToValue {
    fn to_value(&self) -> Value;
}

macro_rules! impl_to_value {
    ($ty:ty, $variant:ident) => {
        impl ToValue for $ty {
            fn to_value(&self) -> Value { Value::$variant(self.to_owned()) }
        }
    };
}

impl_to_value!(bool, Bool);
impl_to_value!(i8, Tinyint);
impl_to_value!(i16, Smallint);
impl_to_value!(i32, Int);
impl_to_value!(i64, Bigint);
impl_to_value!(f32, Float);
impl_to_value!(f64, Double);
impl_to_value!(Vec<u8>, Blob);
impl_to_value!(char, Char);
impl_to_value!(String, Text);
impl_to_value!(Uuid, Uuid);
impl_to_value!(NaiveDate, Date);
impl_to_value!(NaiveTime, Time);
impl_to_value!(DateTime<Utc>, Timestamp);
impl_to_value!(NaiveDateTime, DateTime);

impl ToValue for &str {
    fn to_value(&self) -> Value { Value::Text(self.to_string()) }
}

impl ToValue for Vec<String> {
    fn to_value(&self) -> Value { Value::Array(Array::Text(self.to_owned())) }
}

impl<T> ToValue for Option<T>
where
    T: ToValue,
{
    fn to_value(&self) -> Value {
        match self {
            Some(v) => v.to_value(),
            None => Value::Nil,
        }
    }
}

impl<T> ToValue for &T
where
    T: ToValue,
{
    fn to_value(&self) -> Value { (*self).to_value() }
}

impl<T> From<T> for Value
where
    T: ToValue,
{
    fn from(v: T) -> Value { v.to_value() }
}

pub trait FromValue: Sized {
    fn from_value(v: &Value) -> Result<Self, ConvertError>;
}

macro_rules! impl_from_value {
    ($ty: ty, $ty_name: tt, $($variant: ident),*) => {
        /// try from to owned
        impl FromValue for $ty {
            fn from_value(v: &Value) -> Result<Self, ConvertError> {
                match *v {
                    $(Value::$variant(ref v) => Ok(v.to_owned() as $ty),
                    )*
                    _ => Err(ConvertError::NotSupported(format!("{:?}",v), $ty_name.into())),
                }
            }
        }
    }
}

macro_rules! impl_from_value_numeric {
    ($ty: ty, $method:ident, $ty_name: tt, $($variant: ident),*) => {
        impl FromValue for $ty {
            fn from_value(v: &Value) -> Result<Self, ConvertError> {
                match *v {
                    $(Value::$variant(ref v) => Ok(v.to_owned() as $ty),
                    )*
                    Value::BigDecimal(ref v) => Ok(v.$method().unwrap()),
                    _ => Err(ConvertError::NotSupported(format!("{:?}", v), $ty_name.into())),
                }
            }
        }
    }
}

impl_from_value!(Vec<u8>, "Vec<u8>", Blob);
impl_from_value!(char, "char", Char);
impl_from_value!(Uuid, "Uuid", Uuid);
impl_from_value!(NaiveDate, "NaiveDate", Date);
impl_from_value_numeric!(i8, to_i8, "i8", Tinyint);
impl_from_value_numeric!(i16, to_i16, "i16", Tinyint, Smallint);
impl_from_value_numeric!(i32, to_i32, "i32", Tinyint, Smallint, Int, Bigint);
impl_from_value_numeric!(i64, to_i64, "i64", Tinyint, Smallint, Int, Bigint);
impl_from_value_numeric!(f32, to_f32, "f32", Float);
impl_from_value_numeric!(f64, to_f64, "f64", Float, Double);

/// Char can be casted into String
/// and they havea separate implementation for extracting data
impl FromValue for String {
    fn from_value(v: &Value) -> Result<Self, ConvertError> {
        match *v {
            Value::Text(ref v) => Ok(v.to_owned()),
            Value::Char(ref v) => {
                let mut s = String::new();
                s.push(*v);
                Ok(s)
            }
            Value::Blob(ref v) => {
                String::from_utf8(v.to_owned()).map_err(|e| {
                    ConvertError::NotSupported(format!("{:?}", v), format!("String: {}", e))
                })
            }
            _ => {
                Err(ConvertError::NotSupported(
                    format!("{:?}", v),
                    "String".to_string(),
                ))
            }
        }
    }
}

impl FromValue for Vec<String> {
    fn from_value(v: &Value) -> Result<Self, ConvertError> {
        match *v {
            Value::Array(Array::Text(ref t)) => Ok(t.to_owned()),
            _ => {
                Err(ConvertError::NotSupported(
                    format!("{:?}", v),
                    "Vec<String>".to_string(),
                ))
            }
        }
    }
}

impl FromValue for bool {
    fn from_value(v: &Value) -> Result<Self, ConvertError> {
        match *v {
            Value::Bool(v) => Ok(v),
            Value::Tinyint(v) => Ok(v == 1),
            Value::Smallint(v) => Ok(v == 1),
            Value::Int(v) => Ok(v == 1),
            Value::Bigint(v) => Ok(v == 1),
            _ => {
                Err(ConvertError::NotSupported(
                    format!("{:?}", v),
                    "bool".to_string(),
                ))
            }
        }
    }
}

impl FromValue for DateTime<Utc> {
    fn from_value(v: &Value) -> Result<Self, ConvertError> {
        match *v {
            Value::Text(ref v) => Ok(DateTime::<Utc>::from_utc(parse_naive_date_time(v), Utc)),
            Value::DateTime(v) => Ok(DateTime::<Utc>::from_utc(v, Utc)),
            Value::Timestamp(v) => Ok(v),
            _ => {
                Err(ConvertError::NotSupported(
                    format!("{:?}", v),
                    "DateTime".to_string(),
                ))
            }
        }
    }
}

impl FromValue for NaiveDateTime {
    fn from_value(v: &Value) -> Result<Self, ConvertError> {
        match *v {
            Value::Text(ref v) => Ok(parse_naive_date_time(v)),
            Value::DateTime(v) => Ok(v),
            _ => {
                Err(ConvertError::NotSupported(
                    format!("{:?}", v),
                    "NaiveDateTime".to_string(),
                ))
            }
        }
    }
}

impl<T> FromValue for Option<T>
where
    T: FromValue,
{
    fn from_value(v: &Value) -> Result<Self, ConvertError> {
        match *v {
            Value::Nil => Ok(None),
            _ => FromValue::from_value(v).map(Some),
        }
    }
}

fn parse_naive_date_time(v: &str) -> NaiveDateTime {
    let ts = NaiveDateTime::parse_from_str(&v, "%Y-%m-%d %H:%M:%S");
    if let Ok(ts) = ts {
        ts
    } else {
        let ts = NaiveDateTime::parse_from_str(&v, "%Y-%m-%d %H:%M:%S.%f");
        if let Ok(ts) = ts {
            ts
        } else {
            panic!("unable to parse timestamp: {}", v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::offset::Utc;
    use std::mem::size_of;

    #[test]
    fn data_sizes() {
        assert_eq!(48, size_of::<Value>()); // use to be 32, now 48 due to the addition of BigDecimal type
        assert_eq!(24, size_of::<Vec<u8>>());
        assert_eq!(24, size_of::<String>());
        assert_eq!(12, size_of::<DateTime<Utc>>());
        assert_eq!(4, size_of::<NaiveDate>());
        assert_eq!(16, size_of::<Uuid>());
    }

    #[test]
    fn test_types() {
        let _: Value = 127i8.to_value();
        let _: Value = 2222i16.to_value();
        let _: Value = 4444i32.to_value();
        let _: Value = 10000i64.to_value();
        let _v1: Value = 1.0f32.to_value();
        let _v2: Value = 100.0f64.to_value();
        let _v3: Value = Utc::now().to_value();
        let _v7: Value = Utc::today().naive_utc().to_value();
        let _v4: Value = "hello world!".to_value();
        let _v5: Value = "hello world!".to_string().to_value();
        let _v6: Value = vec![1u8, 2, 255, 3].to_value();
    }

    #[test]
    fn naive_date_parse() {
        let v = "2018-01-29";
        let ts = NaiveDate::parse_from_str(v, "%Y-%m-%d");
        println!("{:?}", ts);
        assert!(ts.is_ok());
    }

    #[test]
    fn naive_date_time_parse() {
        let v = "2018-01-29 09:58:20";
        let ts = NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S");
        println!("{:?}", ts);
        assert!(ts.is_ok());
    }

    #[test]
    fn date_time_conversion() {
        let v = "2018-01-29 09:58:20";
        let ts = NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S");
        println!("{:?}", ts);
        assert!(ts.is_ok());
        DateTime::<Utc>::from_utc(ts.unwrap(), Utc);
    }
}
