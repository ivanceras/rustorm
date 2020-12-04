use crate::{types::SqlType, ColumnName, FromDao, TableName};
use uuid::Uuid;

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnDef {
    pub table: TableName,
    pub name: ColumnName,
    pub comment: Option<String>,
    pub specification: ColumnSpecification,
    pub stat: Option<ColumnStat>,
}

impl ColumnDef {
    /// check all the column constraint if any has AutoIncrement
    pub fn is_autoincrement(&self) -> bool {
        self.autoincrement_sequence_name().is_some()
    }

    /// get the sequnce name of this autoincrement column
    pub fn autoincrement_sequence_name(&self) -> Option<&String> {
        self.specification
            .constraints
            .iter()
            .find_map(|c| match &c {
                ColumnConstraint::AutoIncrement(sequence_name) => sequence_name.as_ref(),
                _ => None,
            })
    }

    /// check if any of the column constraint default is generated from uuid
    pub fn default_is_generated_uuid(&self) -> bool {
        self.specification.constraints.iter().any(|c| match *c {
            ColumnConstraint::DefaultValue(ref literal) => match *literal {
                Literal::UuidGenerateV4 => true,
                _ => false,
            },
            _ => false,
        })
    }

    pub fn is_not_null(&self) -> bool {
        self.specification.constraints.iter().any(|c| match *c {
            ColumnConstraint::NotNull => true,
            _ => false,
        })
    }

    pub fn get_sql_type(&self) -> SqlType {
        self.specification.sql_type.clone()
    }

    pub fn cast_as(&self) -> Option<SqlType> {
        self.get_sql_type().cast_as()
    }

    pub fn has_generated_default(&self) -> bool {
        self.specification.constraints.iter().any(|c| match *c {
            ColumnConstraint::DefaultValue(ref literal) => match *literal {
                Literal::Bool(_) => true,
                Literal::Null => false,
                Literal::Integer(_) => true,
                Literal::Double(_) => true,
                Literal::UuidGenerateV4 => true,
                Literal::Uuid(_) => true,
                Literal::String(_) => false,
                Literal::Blob(_) => false,
                Literal::CurrentTime => true,
                Literal::CurrentDate => true,
                Literal::CurrentTimestamp => true,
                Literal::ArrayInt(_) => false,
                Literal::ArrayFloat(_) => false,
                Literal::ArrayString(_) => false,
            },
            _ => false,
        })
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct ColumnSpecification {
    pub sql_type: SqlType,
    pub capacity: Option<Capacity>,
    pub constraints: Vec<ColumnConstraint>,
}

impl ColumnSpecification {
    pub fn get_limit(&self) -> Option<i32> {
        match self.capacity {
            Some(ref capacity) => capacity.get_limit(),
            None => None,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Capacity {
    Limit(i32),
    Range(i32, i32),
}

impl Capacity {
    fn get_limit(&self) -> Option<i32> {
        match *self {
            Capacity::Limit(limit) => Some(limit),
            Capacity::Range(_whole, _decimal) => None,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum ColumnConstraint {
    NotNull,
    DefaultValue(Literal),
    /// the string contains the sequence name of this serial column
    AutoIncrement(Option<String>),
}

#[derive(Debug, PartialEq, Clone)]
pub enum Literal {
    Bool(bool),
    Null,
    Integer(i64),
    Double(f64),
    UuidGenerateV4, // pg: uuid_generate_v4();
    Uuid(Uuid),
    String(String),
    Blob(Vec<u8>),
    CurrentTime,      // pg: now()
    CurrentDate,      //pg: today()
    CurrentTimestamp, // pg: now()
    ArrayInt(Vec<i64>),
    ArrayFloat(Vec<f64>),
    ArrayString(Vec<String>),
}

/// column stat, derive from pg_stats
#[derive(Debug, PartialEq, FromDao, Clone)]
pub struct ColumnStat {
    pub avg_width: i32, /* average width of the column, (the number of characters) */
    //most_common_values: Value,//top 5 most common values
    pub n_distinct: f32, // the number of distinct values of these column
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
