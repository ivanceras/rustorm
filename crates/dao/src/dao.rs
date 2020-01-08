use crate::{
    error::DaoError,
    FromValue,
    ToValue,
    Value,
};
use serde::{
    ser::{
        Serialize,
        Serializer,
    },
    Deserialize,
    Deserializer,
};
use std::collections::BTreeMap;

#[derive(Debug, PartialEq, Clone, Default)]
pub struct Dao(pub BTreeMap<String, Value>);

impl Dao {
    pub fn new() -> Self { Dao::default() }

    pub fn insert<K, V>(&mut self, k: K, v: V)
    where
        K: ToString,
        V: ToValue,
    {
        self.0.insert(k.to_string(), v.to_value());
    }

    pub fn insert_value<K>(&mut self, k: K, value: &Value)
    where
        K: ToString,
    {
        self.0.insert(k.to_string(), value.clone());
    }

    pub fn get<'a, T>(&'a self, s: &str) -> Result<T, DaoError>
    where
        T: FromValue,
    {
        let value: Option<&'a Value> = self.0.get(s);
        match value {
            Some(v) => FromValue::from_value(v).map_err(DaoError::ConvertError),
            None => Err(DaoError::NoSuchValueError(s.into())),
        }
    }

    pub fn get_opt<'a, T>(&'a self, s: &str) -> Result<Option<T>, DaoError>
    where
        T: FromValue,
    {
        let value: Option<&'a Value> = self.0.get(s);
        match value {
            Some(v) => {
                match v {
                    Value::Nil => Ok(None),
                    _ => {
                        Ok(Some(
                            FromValue::from_value(v).map_err(DaoError::ConvertError)?,
                        ))
                    }
                }
            }
            None => Ok(None),
        }
    }

    pub fn get_value(&self, s: &str) -> Option<&Value> { self.0.get(s) }

    pub fn remove(&mut self, s: &str) -> Option<Value> { self.0.remove(s) }
}

impl<'a> Serialize for Dao {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Dao {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        BTreeMap::deserialize(deserializer).map(Dao)
    }
}

pub trait FromDao {
    /// convert dao to an instance of the corresponding struct of the model
    /// taking into considerating the renamed columns
    fn from_dao(dao: &Dao) -> Self;
}

pub trait ToDao {
    /// convert from an instance of the struct to a dao representation
    /// to be saved into the database
    fn to_dao(&self) -> Dao;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use uuid::Uuid;

    #[test]
    fn insert_double() {
        let mut dao = Dao::new();
        dao.insert("life", 42.0f64);
        let life: Result<f64, DaoError> = dao.get("life");
        assert_eq!(life.unwrap(), 42.0f64);
    }

    #[test]
    fn insert_float() {
        let mut dao = Dao::new();
        dao.insert("life", 42.0f32);
        let life: Result<f64, DaoError> = dao.get("life");
        assert_eq!(life.unwrap(), 42.0f64);
    }

    #[test]
    fn uuid() {
        let mut dao = Dao::new();
        let uuid = Uuid::new_v4();
        dao.insert("user_id", uuid);
    }

    #[test]
    fn serialize_json() {
        let mut dao = Dao::new();
        dao.insert("life", 42);
        dao.insert("lemons", "lemonade");
        let json = serde_json::to_string(&dao).unwrap();
        let expected = r#"{"lemons":{"Text":"lemonade"},"life":{"Int":42}}"#;
        assert_eq!(json, expected);
    }

    #[test]
    fn test_get_opt() {
        let mut dao = Dao::new();
        dao.insert("life", 42);
        let life: Result<Option<i32>, _> = dao.get("life");
        assert!(life.is_ok());
        let life = life.unwrap();
        assert!(life.is_some());
        assert_eq!(life.unwrap(), 42);
    }

    #[test]
    fn referenced() {
        let mut dao = Dao::new();
        let v = 42;
        let s = "lemonade";
        dao.insert("life", &v);
        dao.insert("lemons", s);
        let life: Result<Option<i32>, _> = dao.get("life");
        assert!(life.is_ok());
        let life = life.unwrap();
        assert!(life.is_some());
        assert_eq!(life.unwrap(), 42);
    }
}
