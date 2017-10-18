use dao::TableName;
use dao::ColumnName;


pub struct Foreign {
    pub table: TableName,
    pub column: Vec<ColumnName>,
}
