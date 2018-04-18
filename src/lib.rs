#![allow(dead_code)]
#![deny(unused_must_use)]

#![feature(match_default_bindings)]
#![feature(const_fn)]
#![feature(decl_macro)]

extern crate reduce;

use std::path::{Path, PathBuf};
use std::io;

use std::collections::HashMap;

pub mod table;
pub mod field;
pub mod value;
pub mod query;
pub mod function;

pub mod builtin_functions;

pub use table::{Table, TableField, Row};
pub use field::{Field, FieldKind, IntSize};
pub use value::Value;
pub use query::{Query, QueryField, QueryResult};
pub use function::{FunctionCall, Argument};

use function::Function;

pub type TableName = String;
pub type FieldName = String;
pub type FunctionName = String;

#[derive(Debug, Clone)]
pub enum TypeError {
    NotBoolean,
}

#[derive(Debug, Clone)]
pub enum QueryError {
    IncompatibleTypes,
    DifferentFields,
    TypeError(TypeError),
    NotEnoughArguments(usize),
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
    table_rows: HashMap<TableName, Vec<Row>>,
    // indexes: Vec<(TableName, TableIndex)>,
    functions: HashMap<FunctionName, Function>
}
impl DataDB {
    pub(crate) fn new() -> Self {
        let mut functions: HashMap<FunctionName, Function> = HashMap::new();
        for (name, function) in builtin_functions::FUNCTIONS.iter() {
            functions.insert(name.to_owned().to_owned(), Function::Native(function.clone()));
        }

        Self {
            tables: Vec::new(),
            table_rows: HashMap::new(),
            functions,
        }
    }

    pub(crate) fn function_dict(&self) -> HashMap<FunctionName, Function> {
        self.functions.clone()
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

    /// Companies (INT id, TEXT name, TEXT city)
    /// Employees (INT id, TEXT name, TEXT company)
    fn setup_simple_company_employee_scenario() -> SrimDB {

        let mut db = SrimDB::new();

        db.apply(Delta::CreateTable(
            Table::new("Companies", vec![
                TableField::new("id".to_owned(),   FieldKind::Integer(IntSize::N64, false)),
                TableField::new("name".to_owned(), FieldKind::Text),
                TableField::new("city".to_owned(), FieldKind::Text),
            ])
        )).unwrap();

        db.apply(Delta::CreateTable(
            Table::new("Employees", vec![
                TableField::new("id".to_owned(),      FieldKind::Integer(IntSize::N64, false)),
                TableField::new("name".to_owned(),    FieldKind::Text),
                TableField::new("company".to_owned(), FieldKind::Text),
            ])
        )).unwrap();

        const EMPLOYEE_COUNT: usize  = 500;
        const COMPANY_COUNT:  usize  = 100;
        const CITY_COUNT:     usize  =  10;

        for i in 0..COMPANY_COUNT {
            db.apply(Delta::AddRow(
                "Companies".to_owned(),
                Row::new(vec![
                    Value::Unsigned(i as u128),
                    Value::Text(format!("Company {}", i).to_owned()),
                    Value::Text(format!("City {}", i % CITY_COUNT).to_owned()),
                ])
            )).unwrap();
        }

        for i in 0..EMPLOYEE_COUNT {
            db.apply(Delta::AddRow(
                "Employees".to_owned(),
                Row::new(vec![
                    Value::Unsigned(i as u128),
                    Value::Text(format!("Person {}", i).to_owned()),
                    Value::Text(format!("Company {}", i % COMPANY_COUNT).to_owned()),
                ])
            )).unwrap();
        }

        db
    }

    #[test]
    fn test_simple() {
        let mut db = SrimDB::new().with_path("test.db");

        db.apply(Delta::CreateTable(
            Table::new("Users", vec![
                TableField::new("id".to_owned(),   FieldKind::Integer(IntSize::N64, false)),
                TableField::new("name".to_owned(), FieldKind::Text),
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

    #[test]
    fn test_query_math() {
        let result = SrimDB::new().query(
            Query::FromFunctionCall(
                TableField::new("sum".to_owned(), FieldKind::Integer(IntSize::N32, true)),
                FunctionCall::new("add".to_owned(), vec![
                    Argument::Value(Value::Signed(2)),
                    Argument::Value(Value::Signed(3)),
                    Argument::Value(Value::Signed(-4)),
                ])
            )
        ).unwrap();

        assert_eq!(result.rows(), vec![Row::new(vec![Value::Signed(1)])]);
    }

    #[test]
    fn test_simple_set_ops() {
        let v1 = Query::FromValue(TableField::new("value".to_owned(), FieldKind::Integer(IntSize::N32, true)), Value::Signed(1));
        let v2 = Query::FromValue(TableField::new("value".to_owned(), FieldKind::Integer(IntSize::N32, true)), Value::Signed(2));

        let result = SrimDB::new().query(v1.clone()).unwrap();
        assert_eq!(result.rows(), vec![Row::new(vec![Value::Signed(1)])]);

        // Union
        let result = SrimDB::new().query(Query::Union(Box::new(v1.clone()), Box::new(Query::Empty(vec!["value".to_owned()])))).unwrap();
        assert_eq!(result.rows(), vec![Row::new(vec![Value::Signed(1)])]);

        let result = SrimDB::new().query(Query::Union(Box::new(v1.clone()), Box::new(v2.clone()))).unwrap();
        assert_eq!(result.rows(), vec![Row::new(vec![Value::Signed(1)]), Row::new(vec![Value::Signed(2)])]);

        // Intersection
        let result = SrimDB::new().query(Query::Intersection(Box::new(v1.clone()), Box::new(Query::Empty(vec!["value".to_owned()])))).unwrap();
        assert_eq!(result.rows(), vec![]);

        let result = SrimDB::new().query(Query::Intersection(Box::new(v1.clone()), Box::new(v1.clone()))).unwrap();
        assert_eq!(result.rows(), vec![Row::new(vec![Value::Signed(1)])]);

        let result = SrimDB::new().query(Query::Intersection(
            Box::new(Query::Union(Box::new(v1.clone()), Box::new(v2.clone()))),
            Box::new(v1.clone())
        )).unwrap();
        assert_eq!(result.rows(), vec![Row::new(vec![Value::Signed(1)])]);

        let result = SrimDB::new().query(Query::Intersection(
            Box::new(Query::Union(Box::new(v1.clone()), Box::new(v2.clone()))),
            Box::new(Query::Union(Box::new(v1.clone()), Box::new(v2.clone()))),
        )).unwrap();
        assert_eq!(result.rows(), vec![Row::new(vec![Value::Signed(1)]), Row::new(vec![Value::Signed(2)])]);

        // Difference
        let result = SrimDB::new().query(Query::Difference(Box::new(v1.clone()), Box::new(Query::Empty(vec!["value".to_owned()])))).unwrap();
        assert_eq!(result.rows(), vec![Row::new(vec![Value::Signed(1)])]);

        let result = SrimDB::new().query(Query::Difference(Box::new(v1.clone()), Box::new(v1.clone()))).unwrap();
        assert_eq!(result.rows(), vec![]);

        let result = SrimDB::new().query(Query::Difference(
            Box::new(Query::Union(Box::new(v1.clone()), Box::new(v2.clone()))),
            Box::new(v1.clone())
        )).unwrap();
        assert_eq!(result.rows(), vec![Row::new(vec![Value::Signed(2)])]);

        let result = SrimDB::new().query(Query::Difference(
            Box::new(Query::Union(Box::new(v1.clone()), Box::new(v2.clone()))),
            Box::new(Query::Union(Box::new(v1.clone()), Box::new(v2.clone()))),
        )).unwrap();
        assert_eq!(result.rows(), vec![]);
    }

    #[test]
    fn test_select_condition() {
        let db = setup_simple_company_employee_scenario();

        let company_names_and_cities = Query::Project(
            vec![
                QueryField::new("name".to_owned()),
                QueryField::new("city".to_owned()),
            ],
            Box::new(Query::Table(
                "Companies".to_owned()
            ))
        );

        // Discard all
        let result = db.query(
            Query::Select(query::Condition::Value(Value::Boolean(false)), Box::new(company_names_and_cities.clone()))
        ).unwrap();

        assert_eq!(result.rows().len(), 0);

        // Select all
        let result = db.query(
            Query::Select(query::Condition::Value(Value::Boolean(true)), Box::new(company_names_and_cities.clone()))
        ).unwrap();

        assert_eq!(result.rows().len(), 100);

        // Select "City 2"
        let result = db.query(
            Query::Select(
                query::Condition::FunctionCall(
                    FunctionCall::new("strict_eq".to_owned(), vec![
                        Argument::QueryField(QueryField::new("city".to_owned())),
                        Argument::Value(Value::Text("City 2".to_owned()))
                    ])
                ),
                Box::new(company_names_and_cities)
            )
        ).unwrap();

        assert_eq!(result.rows().len(), 10);
    }

    #[test]
    fn test_rename() {
        let db = setup_simple_company_employee_scenario();

        let company_names_and_cities = Query::Project(
            vec![
                QueryField::new("name".to_owned()),
                QueryField::new("city".to_owned()),
            ],
            Box::new(Query::Table(
                "Companies".to_owned()
            ))
        );

        let result = db.query(Query::Rename(QueryField::new("name".to_owned()), "company".to_owned(), Box::new(company_names_and_cities))).unwrap();
        assert_eq!(result.field_names(), vec!["company", "city"]);
    }

    // #[test]
    // fn test_join_rename() {
    //     // Find out names of all people working for companies in "City 2"
    //     //
    //     // SELECT Employees.name
    //     // FROM (
    //     //     SELECT Employees.name, Companies.city
    //     //     FROM Employees
    //     //     JOIN Companies
    //     //         ON Companies.name == Employees.company
    //     //     WHERE Companies.city == "City 2"
    //     // )
    //
    //     let db = setup_simple_company_employee_scenario();
    //
    //     let result = db.query(
    //         Query::Project(
    //             vec![QueryField::new("name".to_owned())],
    //             Box::new(Query::Table(
    //                 "Companies".to_owned()
    //             ))
    //         )
    //     ).unwrap();
    // }
}
