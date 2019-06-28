use crate::common;
use serde_derive::{
    Deserialize,
    Serialize,
};

#[derive(Debug, Serialize,Deserialize,  PartialEq, Clone)]
pub struct ColumnName {
    pub name: String,
    pub table: Option<String>,
    pub alias: Option<String>,
}

impl ColumnName {
    /// create table with name
    pub fn from(arg: &str) -> Self {
        if arg.contains('.') {
            let splinters = arg.split('.').collect::<Vec<&str>>();
            assert!(
                splinters.len() == 2,
                "There should only be 2 parts, trying to split `.` {}",
                arg
            );
            let table = splinters[0].to_owned();
            let name = splinters[1].to_owned();
            ColumnName {
                name,
                table: Some(table),
                alias: None,
            }
        } else {
            ColumnName {
                name: arg.to_owned(),
                table: None,
                alias: None,
            }
        }
    }

    /// return the long name of the table using schema.table_name
    pub fn complete_name(&self) -> String {
        match self.table {
            Some(ref table) => format!("{}.{}", table, self.name),
            None => self.name.to_owned(),
        }
    }

    pub fn safe_complete_name(&self) -> String {
        match self.table {
            Some(ref table) => format!("{}.{}", common::keywords_safe(table), self.name),
            None => self.name.to_owned(),
        }
    }
}

pub trait ToColumnNames {
    /// extract the columns from struct
    fn to_column_names() -> Vec<ColumnName>;
}
