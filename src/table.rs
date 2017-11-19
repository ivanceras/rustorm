use dao::TableName;
use column::Column;
use dao::ColumnName;

#[derive(Debug, PartialEq, Clone)]
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

impl Table {

    pub fn complete_name(&self) -> String {
        self.name.complete_name()
    }

    pub fn get_primary_columns(&self) -> Vec<&ColumnName> {
        let mut primary:Vec<&ColumnName> = vec![];
        for key in &self.table_key{
            match *key{
                TableKey::PrimaryKey(ref pk) => 
                {
                    for col in &pk.columns{
                        primary.push(col)
                    }
                }
                _ => (),
            }
        }
        primary
    }

    pub fn get_foreign_keys(&self) -> Vec<&ForeignKey> {
        let mut foreign:Vec<&ForeignKey> = vec![];
        for key in &self.table_key{
            match *key{
                TableKey::ForeignKey(ref fk) => 
                    foreign.push(fk),     
                _ => (),
            }
        }
        foreign
    }

    pub fn get_foreign_key_to_table(&self, table_name: &TableName) -> Option<&ForeignKey> {
        let foreign_keys:Vec<&ForeignKey> = self.get_foreign_keys();
        for fk in foreign_keys{
            if fk.foreign_table == *table_name{
                return Some(fk)
            }
        }
        None
    }

    pub fn get_foreign_columns(&self) -> Vec<&ColumnName> {
        let mut foreign_columns = vec![];
        let foreign_keys = self.get_foreign_keys();
        for fk in &foreign_keys{
            for fk_column in &fk.columns{
                foreign_columns.push(fk_column);
            }
        }
        foreign_columns
    }

    pub fn get_column(&self, column_name: &ColumnName) -> Option<&Column> {
        self.columns.iter()
            .find(|c|c.name == *column_name)
    }
}


#[derive(Debug, PartialEq, Clone)]
pub struct PrimaryKey{
    pub name: Option<String>,
    pub columns: Vec<ColumnName>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct UniqueKey{
    pub name: Option<String>,
    pub columns: Vec<ColumnName>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct ForeignKey{
    pub name: Option<String>,
    pub columns: Vec<ColumnName>,
    // referred foreign table
    pub foreign_table: TableName,
    // referred column of the foreign table
    pub referred_columns: Vec<ColumnName>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Key{
    pub name: Option<String>,
    pub columns: Vec<ColumnName>,
}

#[derive(Debug, PartialEq, Clone)]
pub enum TableKey {
    PrimaryKey(PrimaryKey),
    UniqueKey(UniqueKey),
    Key(Key),
    ForeignKey(ForeignKey),
}


#[derive(Debug)]
pub struct SchemaContent {
    pub schema: String,
    pub tablenames: Vec<TableName>,
    pub views: Vec<TableName>
}

