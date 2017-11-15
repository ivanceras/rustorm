
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TableName {
    pub name: String,
    pub schema: Option<String>,
    pub alias: Option<String>,
}

impl TableName {

    /// create table with name
    pub fn from(arg: &str) -> Self {
        if arg.contains(".") {
            let splinters = arg.split(".").collect::<Vec<&str>>();
            assert!(splinters.len() == 2, "There should only be 2 parts");
            let schema = splinters[0].to_owned();
            let table = splinters[1].to_owned();
            TableName {
                schema: Some(schema),
                name: table,
                alias: None,
            }
        } else {
            TableName {
                schema: None,
                name: arg.to_owned(),
                alias: None,
            }
        }
    }

    /// return the long name of the table using schema.table_name
    pub fn complete_name(&self) -> String {
        match self.schema {
            Some(ref schema) => format!("{}.{}", schema, self.name),
            None => self.name.to_owned(),
        }
    }
}

pub trait ToTableName {
    /// extract the table name from a struct
    fn to_table_name() -> TableName;
}
