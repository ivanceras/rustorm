use crate::{
    Dao,
    Value,
};
use serde_derive::{
    Deserialize,
    Serialize,
};
use std::slice;

/// use this to store data retrieved from the database
/// This is also slimmer than Vec<Dao> when serialized
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Rows {
    pub columns: Vec<String>,
    pub data: Vec<Vec<Value>>,
    /// can be optionally set, indicates how many total rows are there in the table
    pub count: Option<usize>,
}

impl Rows {
    pub fn empty() -> Self { Rows::new(vec![]) }

    pub fn new(columns: Vec<String>) -> Self {
        Rows {
            columns,
            data: vec![],
            count: None,
        }
    }

    pub fn push(&mut self, row: Vec<Value>) { self.data.push(row) }

    /// Returns an iterator over the `Row`s.
    pub fn iter(&self) -> Iter {
        Iter {
            columns: self.columns.clone(),
            iter: self.data.iter(),
        }
    }
}

/// An iterator over `Row`s.
pub struct Iter<'a> {
    columns: Vec<String>,
    iter: slice::Iter<'a, Vec<Value>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Dao;

    fn next(&mut self) -> Option<Dao> {
        let next_row = self.iter.next();
        if let Some(row) = next_row {
            if !row.is_empty() {
                let mut dao = Dao::new();
                for (i, column) in self.columns.iter().enumerate() {
                    if let Some(value) = row.get(i) {
                        dao.insert_value(column, value);
                    }
                }
                Some(dao)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) { self.iter.size_hint() }
}

impl<'a> ExactSizeIterator for Iter<'a> {}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn iteration_count() {
        let columns = vec!["id".to_string(), "username".to_string()];
        let data: Vec<Vec<Value>> = vec![vec![1.into(), "ivanceras".into()]];
        let rows = Rows {
            columns,
            data,
            count: None,
        };
        assert_eq!(1, rows.iter().count());
    }

    #[test]
    fn iteration_count2() {
        let columns = vec!["id".to_string(), "username".to_string()];
        let data: Vec<Vec<Value>> = vec![vec![1.into(), "ivanceras".into()], vec![
            2.into(),
            "lee".into(),
        ]];
        let rows = Rows {
            columns,
            data,
            count: None,
        };
        assert_eq!(2, rows.iter().count());
    }

    #[test]
    fn dao() {
        let columns = vec!["id".to_string(), "username".to_string()];
        let data: Vec<Vec<Value>> = vec![vec![1.into(), "ivanceras".into()]];
        let rows = Rows {
            columns,
            data,
            count: None,
        };
        let mut dao = Dao::new();
        dao.insert("id", 1);
        dao.insert("username", "ivanceras");
        assert_eq!(dao, rows.iter().next().unwrap());
    }

    #[test]
    fn dao2() {
        let columns = vec!["id".to_string(), "username".to_string()];
        let data: Vec<Vec<Value>> = vec![vec![1.into(), "ivanceras".into()], vec![
            2.into(),
            "lee".into(),
        ]];
        let rows = Rows {
            columns,
            data,
            count: None,
        };
        let mut iter = rows.iter();
        let mut dao = Dao::new();
        dao.insert("id", 1);
        dao.insert("username", "ivanceras");
        assert_eq!(dao, iter.next().unwrap());

        let mut dao2 = Dao::new();
        dao2.insert("id", 2);
        dao2.insert("username", "lee");
        assert_eq!(dao2, iter.next().unwrap());
    }
}
