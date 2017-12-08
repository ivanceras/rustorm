use std::collections::BTreeMap;
use dao::Dao;
use dao::Value;
use serde::ser::{Serialize, Serializer};

#[derive(Debug)]
pub struct Record(pub BTreeMap<String, Value>);

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
