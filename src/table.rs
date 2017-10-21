use dao::TableName;
use column::Column;
use column::TableKey;

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

