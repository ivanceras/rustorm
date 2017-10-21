use dao::TableName;
use dao::ColumnName;
use types::SqlType;
use foreign::Foreign;
use uuid::Uuid;

#[derive(Debug, PartialEq)]
pub struct Column {
    pub table: Option<TableName>,
    pub name: ColumnName,
    pub comment: Option<String>,
    pub specification: ColumnSpecification,
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


#[derive(Debug)]
pub struct PrimaryKey{
    name: Option<String>,
    columns: Vec<ColumnName>,
}

#[derive(Debug)]
pub struct UniqueKey{
    name: Option<String>,
    columns: Vec<ColumnName>,
}

#[derive(Debug)]
pub struct ForeignKey{
    name: Option<String>,
    table: TableName,
    column: ColumnName,
}

#[derive(Debug)]
pub struct Key{
    name: String,
    columns: Vec<ColumnName>,
}

#[derive(Debug)]
pub enum TableKey {
    PrimaryKey(PrimaryKey),
    UniqueKey(UniqueKey),
    Key(Key),
    ForeignKey(ForeignKey),
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


