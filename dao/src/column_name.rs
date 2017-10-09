


pub struct ColumnName {
    pub name: String,
    pub table: Option<String>,
    pub alias: Option<String>,
}


pub trait ToColumnNames {
    /// extract the columns from struct
    fn to_column_names() -> Vec<ColumnName>;
}
