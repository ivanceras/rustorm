use serde_derive::{
    Deserialize,
    Serialize,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Interval {
    pub microseconds: i64,
    pub days: i32,
    pub months: i32,
}

impl Interval {
    pub fn new(microseconds: i64, days: i32, months: i32) -> Self {
        Interval {
            microseconds,
            days,
            months,
        }
    }
}
