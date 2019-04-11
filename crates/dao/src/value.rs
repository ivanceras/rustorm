use crate::interval::Interval;
use crate::ConvertError;
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use geo::Point;
use serde_derive::{Deserialize, Serialize};
use std::convert::TryFrom;
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
    pub fn is_nil(&self) -> bool {
        *self == Value::Nil
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
    ($ty:ty) => {
        impl ToValue for $ty {
            fn to_value(&self) -> Value {
                self.into()
            }
        }

        impl<'a> ToValue for &'a $ty {
            fn to_value(&self) -> Value {
                (*self).into()
            }
        }
    };
}

impl ToValue for Vec<String> {
    fn to_value(&self) -> Value {
        Value::Array(Array::Text(self.to_owned()))
    }
}

macro_rules! impl_from {
    ($ty:ty, $variant:ident) => {
        /// Owned types
        impl From<$ty> for Value {
            fn from(f: $ty) -> Self {
                Value::$variant(f)
            }
        }

        /// For borrowed types
        impl<'a> From<&'a $ty> for Value {
            fn from(f: &'a $ty) -> Self {
                Value::$variant(f.to_owned())
            }
        }

        /// for borrowed option types
        impl<'a> From<&'a Option<$ty>> for Value {
            fn from(f: &'a Option<$ty>) -> Self {
                match *f {
                    Some(ref f) => From::from(f),
                    None => Value::Nil,
                }
            }
        }

        impl_to_value!($ty);
    };

    ($ty:ty, $variant:ident, $fn:ident) => {
        /// Owned types
        impl From<$ty> for Value {
            fn from(f: $ty) -> Self {
                Value::$variant(f.$fn())
            }
        }

        /// For borrowed types
        impl<'a> From<&'a $ty> for Value {
            fn from(f: &'a $ty) -> Self {
                Value::$variant(f.$fn())
            }
        }

        /// for borrowed option types
        impl<'a> From<&'a Option<$ty>> for Value {
            fn from(f: &'a Option<$ty>) -> Self {
                match *f {
                    Some(ref f) => From::from(f),
                    None => Value::Nil,
                }
            }
        }

        impl_to_value!($ty);
    };
}

impl_from!(bool, Bool);
impl_from!(i8, Tinyint);
impl_from!(i16, Smallint);
impl_from!(i32, Int);
impl_from!(i64, Bigint);
impl_from!(f32, Float);
impl_from!(f64, Double);
impl_from!(Vec<u8>, Blob);
impl_from!(char, Char);
impl_from!(String, Text);
impl_from!(Uuid, Uuid);
impl_from!(NaiveDate, Date);
impl_from!(NaiveTime, Time);
impl_from!(DateTime<Utc>, Timestamp);

impl<'a> From<&'a str> for Value {
    fn from(f: &'a str) -> Value {
        Value::Text(f.to_string())
    }
}

impl From<Vec<String>> for Value {
    fn from(f: Vec<String>) -> Value {
        Value::Array(Array::Text(f))
    }
}

impl<'a> From<&'a Vec<String>> for Value {
    fn from(f: &Vec<String>) -> Value {
        Value::Array(Array::Text(f.to_owned()))
    }
}

impl<'a> From<&'a Value> for Vec<String> {
    fn from(v: &'a Value) -> Vec<String> {
        match *v {
            Value::Array(Array::Text(ref t)) => t.to_owned(),
            _ => panic!("unable to convert {:?} to Vec<String>", v),
        }
    }
}

macro_rules! impl_tryfrom {
    ($ty: ty, $ty_name: tt, $($variant: ident),*) => {
        /// try from to owned
        impl<'a> TryFrom<&'a Value> for $ty {
            type Error = ConvertError;

            fn try_from(value: &'a Value) -> Result<Self, Self::Error> {
                match *value {
                    $(Value::$variant(ref v) => Ok(v.to_owned() as $ty),
                    )*
                    _ => Err(ConvertError::NotSupported(format!("{:?}",value), $ty_name.into())),
                }
            }
        }

    }
}

macro_rules! impl_tryfrom_option {
    ($ty:ty) => {
        /// try from to Option<T>
        impl<'a> TryFrom<&'a Value> for Option<$ty> {
            type Error = ConvertError;

            fn try_from(value: &'a Value) -> Result<Self, Self::Error> {
                match *value {
                    Value::Nil => Ok(None),
                    _ => TryFrom::try_from(value).map(|v| Some(v)),
                }
            }
        }
    };
}

/// Char can be casted into String
/// and they havea separate implementation for extracting data
impl<'a> TryFrom<&'a Value> for String {
    type Error = ConvertError;

    fn try_from(value: &'a Value) -> Result<Self, Self::Error> {
        match *value {
            Value::Text(ref v) => Ok(v.to_owned()),
            Value::Char(ref v) => {
                let mut s = String::new();
                s.push(*v);
                Ok(s)
            }
            _ => Err(ConvertError::NotSupported(
                format!("{:?}", value),
                "String".to_string(),
            )),
        }
    }
}

impl_tryfrom!(bool, "bool", Bool);
impl_tryfrom!(i8, "i8", Tinyint);
impl_tryfrom!(i16, "i16", Tinyint, Smallint);
impl_tryfrom!(i32, "i32", Tinyint, Smallint, Int, Bigint);
impl_tryfrom!(i64, "i64", Tinyint, Smallint, Int, Bigint);
impl_tryfrom!(f32, "f32", Float);
impl_tryfrom!(f64, "f64", Float, Double);
impl_tryfrom!(Vec<u8>, "Vec<u8>", Blob);
impl_tryfrom!(char, "char", Char);
impl_tryfrom!(Uuid, "Uuid", Uuid);
impl_tryfrom!(NaiveDate, "NaiveDate", Date);

impl<'a> TryFrom<&'a Value> for NaiveDateTime {
    type Error = ConvertError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match *value {
            Value::Text(ref v) => Ok(parse_naive_date_time(v)),
            _ => Err(ConvertError::NotSupported(
                format!("{:?}", value),
                "NaiveDateTime".to_string(),
            )),
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

impl<'a> TryFrom<&'a Value> for DateTime<Utc> {
    type Error = ConvertError;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match *value {
            Value::Text(ref v) => Ok(DateTime::<Utc>::from_utc(parse_naive_date_time(v), Utc)),
            Value::Timestamp(v) => Ok(v),
            _ => Err(ConvertError::NotSupported(
                format!("{:?}", value),
                "DateTime".to_string(),
            )),
        }
    }
}

impl_tryfrom_option!(bool);
impl_tryfrom_option!(i8);
impl_tryfrom_option!(i16);
impl_tryfrom_option!(i32);
impl_tryfrom_option!(i64);
impl_tryfrom_option!(f32);
impl_tryfrom_option!(f64);
impl_tryfrom_option!(Vec<u8>);
impl_tryfrom_option!(char);
impl_tryfrom_option!(String);
impl_tryfrom_option!(Uuid);
impl_tryfrom_option!(NaiveDate);
impl_tryfrom_option!(NaiveDateTime);
impl_tryfrom_option!(DateTime<Utc>);

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
        let _: Value = 127i8.into();
        let _: Value = 2222i16.into();
        let _: Value = 4444i32.into();
        let _: Value = 10000i64.into();
        let _v1: Value = 1.0f32.into();
        let _v2: Value = 100.0f64.into();
        let _v3: Value = Utc::now().into();
        let _v7: Value = Utc::today().naive_utc().into();
        let _v4: Value = "hello world!".into();
        let _v5: Value = "hello world!".to_string().into();
        let _v6: Value = vec![1u8, 2, 255, 3].into();
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
