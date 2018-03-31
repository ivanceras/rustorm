use dao::Dao;
use dao::Value;
use serde::ser::{Serialize, Serializer};
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct Record(pub BTreeMap<String, Value>);

impl Record {
    pub fn new() -> Record {
        Record(BTreeMap::new())
    }

    pub fn get_value(&self, s: &str) -> Option<Value> {
        self.0.get(s).map(|v| v.clone())
    }

    pub fn insert_value(&mut self, s: String, value: Value) {
        self.0.insert(s, value);
    }
}

impl Serialize for Record {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'a, 'b> From<&'b Dao<'a>> for Record {
    fn from(dao: &'b Dao<'a>) -> Self {
        let mut map: BTreeMap<String, Value> = BTreeMap::new();
        for (k, v) in dao.0.iter() {
            map.insert(k.to_string(), v.clone());
        }
        Record(map)
    }
}
