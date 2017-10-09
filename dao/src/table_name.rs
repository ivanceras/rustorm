
#[derive(Debug)]
pub struct TableName {
    pub name: String,
    pub schema: Option<String>,
    pub alias: Option<String>,
}

impl TableName {
    pub fn name(&self) -> String {
        if let Some(ref schema) = self.schema {
            format!("{}.{}", schema, self.name)
        } else {
            format!("{}", self.name)
        }
    }
}

pub trait ToTableName {
    /// extract the table name from a struct
    fn to_table_name() -> TableName;
}
