use dao::TableName;
use dao::ColumnName;
use types::SqlType;
use foreign::Foreign;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub struct Column {
    pub table: TableName,
    pub name: ColumnName,
    pub comment: Option<String>,
    pub specification: ColumnSpecification,
}

impl Column{
    
    /// check all the column constraint if any has AutoIncrement
    pub fn is_autoincrement(&self) -> bool {
        self.specification.constraints
            .iter()
            .any(|c| *c == ColumnConstraint::AutoIncrement) 
    }


    /// check if any of the column constraint default is generated from uuid
    pub fn default_is_generated_uuid(&self) -> bool {
        self.specification.constraints
            .iter()
            .any(|c|{
                match *c {
                    ColumnConstraint::DefaultValue(ref literal) => {
                        match *literal {
                            Literal::UuidGenerateV4 => true,
                            _ => false
                        }
                    },
                    _ => false
                }
            })
    }

}


#[derive(Debug, PartialEq)]
pub struct ColumnSpecification{
    pub sql_type: SqlType,
    pub capacity: Option<Capacity>,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, PartialEq)]
pub enum Capacity{
    Limit(i32),
    Range(i32, i32),
}



#[derive(Debug, PartialEq)]
pub enum ColumnConstraint {
    NotNull,
    DefaultValue(Literal),
    AutoIncrement,
}


#[derive(Debug, PartialEq)]
pub enum Literal {
    Bool(bool),
    Null,
    Integer(i64),
    Double(f64),
    UuidGenerateV4, // pg: uuid_generate_v4();
    Uuid(Uuid),
    String(String),
    Blob(Vec<u8>),
    CurrentTime, // pg: now()
    CurrentDate, //pg: today()
    CurrentTimestamp, // pg: now()
}

impl From<i64> for Literal {
    fn from(i: i64) -> Self {
        Literal::Integer(i)
    }
}

impl From<String> for Literal {
    fn from(s: String) -> Self {
        Literal::String(s)
    }
}

impl<'a> From<&'a str> for Literal {
    fn from(s: &'a str) -> Self {
        Literal::String(String::from(s))
    }
}


