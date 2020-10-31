use crate::{column::Capacity, types::SqlType, Value};
use bigdecimal::BigDecimal;
use chrono::NaiveDateTime;
use log::*;
use num_traits::ToPrimitive;
use std::str::FromStr;

pub fn extract_datatype_with_capacity(data_type: &str) -> (String, Option<Capacity>) {
    let start = data_type.find('(');
    let end = data_type.find(')');
    if let Some(start) = start {
        if let Some(end) = end {
            let dtype = &data_type[0..start];
            let range = &data_type[start + 1..end];
            let capacity = if range.contains(',') {
                let splinters = range.split(',').collect::<Vec<&str>>();
                assert!(splinters.len() == 2, "There should only be 2 parts");
                let range1: Result<i32, _> = splinters[0].parse();
                let range2: Result<i32, _> = splinters[1].parse();
                match range1 {
                    Ok(r1) => match range2 {
                        Ok(r2) => Some(Capacity::Range(r1, r2)),
                        Err(e) => {
                            info!(
                                "error: {} when parsing range2 for data_type: {:?}",
                                e, data_type
                            );
                            None
                        }
                    },
                    Err(e) => {
                        info!(
                            "error: {} when parsing range1 for data_type: {:?}",
                            e, data_type
                        );
                        None
                    }
                }
            } else {
                let limit: Result<i32, _> = range.parse();
                match limit {
                    Ok(limit) => Some(Capacity::Limit(limit)),
                    Err(e) => {
                        info!(
                            "error: {} when parsing limit for data_type: {:?}",
                            e, data_type
                        );
                        None
                    }
                }
            };
            (dtype.to_owned(), capacity)
        } else {
            (data_type.to_owned(), None)
        }
    } else {
        (data_type.to_owned(), None)
    }
}

pub fn cast_type(value: &Value, required_type: &SqlType) -> Value {
    if *value == Value::Nil || required_type.same_type(value) {
        value.to_owned()
    } else {
        match *value {
            Value::Smallint(v) => match *required_type {
                SqlType::Tinyint => Value::Tinyint(v as i8),
                SqlType::Int => Value::Int(i32::from(v)),
                SqlType::Bigint => Value::Bigint(i64::from(v)),
                _ => panic!(
                    "unsupported conversion from {:?} to {:?}",
                    value, required_type
                ),
            },
            Value::Int(v) => match *required_type {
                SqlType::Tinyint => Value::Tinyint(v as i8),
                SqlType::Smallint => Value::Smallint(v as i16),
                SqlType::Bigint => Value::Bigint(i64::from(v)),
                _ => panic!(
                    "unsupported conversion from {:?} to {:?}",
                    value, required_type
                ),
            },
            Value::Bigint(v) => match *required_type {
                SqlType::Tinyint => Value::Tinyint(v as i8),
                SqlType::Smallint => Value::Smallint(v as i16),
                SqlType::Int => Value::Int(v as i32),
                SqlType::Numeric => {
                    let bigdecimal = BigDecimal::from_str(&format!("{}", v));
                    assert!(bigdecimal.is_ok());
                    Value::BigDecimal(bigdecimal.unwrap())
                }
                SqlType::Varchar => Value::Text(format!("{}", v)),
                _ => panic!(
                    "unsupported conversion from {:?} to {:?}",
                    value, required_type
                ),
            },
            Value::BigDecimal(ref v) => match *required_type {
                SqlType::Int => {
                    let ival = v.to_i32();
                    assert!(ival.is_some());
                    let ival = ival.unwrap();
                    Value::Int(ival)
                }
                SqlType::Varchar => Value::Text(format!("{}", v)),
                _ => panic!(
                    "unsupported conversion from {:?} to {:?}",
                    value, required_type
                ),
            },
            Value::Text(ref v) => {
                match *required_type {
                    SqlType::Timestamp => {
                        let ts = NaiveDateTime::parse_from_str(&v, "%Y-%m-%d %H:%M:%S");
                        let ts = if let Ok(ts) = ts {
                            ts
                        } else {
                            let ts = NaiveDateTime::parse_from_str(&v, "%Y-%m-%d %H:%M:%S.%f");
                            if let Ok(ts) = ts {
                                ts
                            } else {
                                panic!("unable to parse timestamp: {}", v);
                            }
                        };
                        Value::DateTime(ts)
                    }
                    SqlType::Char => {
                        assert_eq!(v.len(), 1);
                        Value::Char(v.chars().next().unwrap())
                    }
                    SqlType::Int => {
                        if let Ok(v) = v.parse::<i32>() {
                            Value::Int(v)
                        } else {
                            panic!(
                                "unsupported conversion from {:?} to {:?}",
                                value, required_type
                            );
                        }
                    }
                    // enums will be just Text
                    SqlType::Enum(_, _) => Value::Text(v.to_string()),
                    // ts vector is casted into text and then we just
                    // return them as text as well
                    SqlType::TsVector => Value::Text(v.to_string()),
                    _ => panic!(
                        "unsupported conversion from {:?} to {:?}",
                        value, required_type
                    ),
                }
            }
            Value::ImageUri(_) => {
                info!("passing ImageUri as is");
                value.clone()
            }
            Value::Char(v) => match *required_type {
                SqlType::Varchar => Value::Text(format!("{}", v)),
                _ => panic!(
                    "unsupported conversion from {:?} to {:?}",
                    value, required_type
                ),
            },
            _ => panic!(
                "unsupported conversion from {:?} to {:?}",
                value, required_type
            ),
        }
    }
}
