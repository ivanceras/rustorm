use dao::TableName;
use column::Column;
use dao::ColumnName;

#[derive(Debug, PartialEq)]
pub struct Table {
    pub name: TableName,

    /// comment of this table
    pub comment: Option<String>,

    /// columns of this table
    pub columns: Vec<Column>,

    /// views can also be generated
    pub is_view: bool,

    pub table_key: Vec<TableKey>,

}


#[derive(Debug, PartialEq)]
pub struct PrimaryKey{
    pub name: Option<String>,
    pub columns: Vec<ColumnName>,
}

#[derive(Debug, PartialEq)]
pub struct UniqueKey{
    pub name: Option<String>,
    pub columns: Vec<ColumnName>,
}

#[derive(Debug, PartialEq)]
pub struct ForeignKey{
    pub name: Option<String>,
    pub columns: Vec<ColumnName>,
    // referred foreign table
    pub foreign_table: TableName,
    // referred column of the foreign table
    pub referred_columns: Vec<ColumnName>,
}

#[derive(Debug, PartialEq)]
pub struct Key{
    pub name: Option<String>,
    pub columns: Vec<ColumnName>,
}

#[derive(Debug, PartialEq)]
pub enum TableKey {
    PrimaryKey(PrimaryKey),
    UniqueKey(UniqueKey),
    Key(Key),
    ForeignKey(ForeignKey),
}


#[derive(Debug)]
pub struct SchemaContent {
    pub schema: String,
    pub tables: Vec<Table>,
    pub views: Vec<Table>
}

