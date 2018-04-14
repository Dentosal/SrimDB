#![allow(dead_code)]
#![deny(unused_must_use)]

#![feature(match_default_bindings)]
#![feature(i128_type)]

use std::path::{Path, PathBuf};
use std::io;

use std::collections::HashMap;

pub mod table;
pub mod field;
pub mod value;
pub mod query;

pub use table::{Table, TableField, Row};
pub use field::{Field, FieldKind, IntSize};
pub use value::Value;
pub use query::{Query, QueryField, QueryResult};

pub type TableName = String;
pub type FieldName = String;

#[derive(Debug, Clone)]
pub enum CastError {
    InvalidTypes,
}

#[derive(Debug, Clone)]
pub enum QueryError {
    NoSuchTable(TableName),
    NoSuchField(QueryField),
    AmbiguousField(QueryField),
}

#[derive(Debug, Clone)]
pub enum ApplyError {
    NoSuchTable(TableName),
    AddCannotModify(TableName),
}

pub enum Delta {
    CreateTable(Table),
    DropTable(TableName),
    AddRow(TableName, Row),
    RemoveRow(TableName, Row),
}

#[derive(Clone)]
struct DataDB {
    tables: Vec<Table>,
    table_rows: HashMap<TableName, Vec<Row>>
    // indexes: Vec<(TableName, TableIndex)>,
}
impl DataDB {
    pub(crate) fn new() -> Self {
        Self {
            tables: Vec::new(),
            table_rows: HashMap::new(),
        }
    }

    pub(crate) fn table_index(&self, name: TableName) -> Option<usize> {
        for (i, table) in self.tables.iter().enumerate() {
            if table.name() == name {
                return Some(i);
            }
        }
        None
    }

    pub(crate) fn table_by_index(&self, index: usize) -> Table {
        self.tables[index].clone()
    }

    pub(crate) fn table(&self, name: TableName) -> Option<Table> {
        Some(self.table_by_index(self.table_index(name)?))
    }

    pub(crate) fn all_rows(&self, name: TableName) -> Option<Vec<Row>> {
        self.table_rows.get(&name).map(|x| x.clone())
    }

    pub(crate) fn create_table(&mut self, table: Table) -> Result<(), ApplyError> {
        if let Some(i) = self.table_index(table.name()) {
            if self.tables[i] != table {
                return Err(ApplyError::AddCannotModify(table.name()));
            }
        }
        else {
            self.tables.push(table.clone());
            self.table_rows.insert(table.name(), Vec::new());
        }
        Ok(())
    }

    pub(crate) fn drop_table(&mut self, name: TableName) -> Result<(), ApplyError> {
        if let Some(i) = self.table_index(name.clone()) {
            self.tables.remove(i);
            self.table_rows.remove(&name);
            Ok(())
        }
        else {
            Err(ApplyError::NoSuchTable(name))
        }
    }

    pub(crate) fn add_row(&mut self, name: TableName, row: Row) -> Result<(), ApplyError> {
        self.table_rows
            .get_mut(&name)
            .ok_or(ApplyError::NoSuchTable(name.clone()))?
            .push(row);
        Ok(())
    }
}

pub struct SrimDB {
    filepath: Option<PathBuf>,
    data_db: DataDB
}
impl SrimDB {
    pub fn new() -> Self {
        Self {
            filepath: None,
            data_db: DataDB::new()
        }
    }

    pub fn load<P: AsRef<Path>>(filepath: P) -> io::Result<Self> {
        let mut s = Self::new().with_path(filepath);
        s.load_overwrite()?;
        Ok(s)
    }

    pub fn with_path<P: AsRef<Path>>(self, filepath: P) -> Self {
        assert_eq!(self.filepath, None);
        Self { filepath: Some(filepath.as_ref().to_path_buf()), ..self }
    }


    pub fn load_overwrite(&mut self) -> io::Result<()> {
        unimplemented!();
    }

    pub fn save(&self) -> io::Result<()> {
        println!("SAVE");
        Ok(())
    }

    pub fn query(&self, query: Query) -> Result<QueryResult, QueryError> {
        query.execute(&self.data_db)
    }

    pub fn apply(&mut self, delta: Delta) -> Result<(), ApplyError> {
        use Delta::*;
        match delta {
            CreateTable(table)      => self.data_db.create_table(table),
            DropTable(name)         => self.data_db.drop_table(name),
            AddRow(name, row)       => self.data_db.add_row(name, row),
            _ =>  unimplemented!()
            // DropRow(TableName, Row),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut db = SrimDB::new().with_path("test.db");

        db.apply(Delta::CreateTable(
            Table::new("Users", vec![
                TableField::new("id",   FieldKind::Integer(IntSize::N64, false)),
                TableField::new("name", FieldKind::Text),
            ])
        )).unwrap();

        db.apply(Delta::AddRow(
            "Users".to_owned(),
            Row::new(vec![
                Value::Unsigned(0),
                Value::Text("Test User 1".to_owned())
            ])
        )).unwrap();

        db.apply(Delta::AddRow(
            "Users".to_owned(),
            Row::new(vec![
                Value::Unsigned(1),
                Value::Text("Test User 2".to_owned())
            ])
        )).unwrap();

        let result = db.query(
            Query::Project(
                vec![QueryField::new("name".to_owned())],
                Box::new(Query::Table(
                    "Users".to_owned()
                ))
            )
        ).unwrap();

        assert_eq!(result.field_names(), vec!["name".to_owned()]);
        assert_eq!(result.rows(), vec![
            Row::new(vec![Value::Text("Test User 1".to_owned())]),
            Row::new(vec![Value::Text("Test User 2".to_owned())]),
        ]);

        db.save().unwrap();
    }
}
