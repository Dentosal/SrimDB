use std::fmt;
use std::collections::HashMap;

use TableName;
use FieldName;
use FunctionName;
use TableField;
use Row;
use QueryError;
use DataDB;
use Value;
use TypeError;
use function::{Function, FunctionCall};

#[derive(Debug, Clone)]
pub enum Query {
    /// Table from db
    Table(TableName),

    /// Single column, single row "Table" from a function call
    FromValue(TableField, FunctionCall),

    /// Multiset Union
    Union(Box<Query>, Box<Query>),

    /// Multiset Intersection
    Intersection(Box<Query>, Box<Query>),

    /// Multiset Difference
    Difference(Box<Query>, Box<Query>),

    /// Multiset Product
    Product(Box<Query>, Box<Query>),

    /// Pick fields $0 in $1
    Project(Vec<QueryField>, Box<Query>),

    /// Select (filter) the result set of query
    Select(Condition, Box<Query>),

    /// Rename $0 to $1 in $2
    Rename(QueryField, FieldName, Box<Query>),
}
impl Query {
    pub(crate) fn execute(&self, db: &DataDB) -> Result<QueryResult, QueryError> {
        use Query::*;
        match self {
            Table(name) => QueryResult::from_db_table(&db, name.clone()),
            FromValue(field, fc) => {
                let fd = db.function_dict();
                let value = (*fc).resolve_args(&|_qf: &QueryField| {
                    panic!("FromValue references a field"); // TODO: just return QueryError?
                })?.apply(&fd)?;

                Ok(QueryResult::new(vec![QueryField::new(field.name())], vec![Row::new(vec![value])]))
            },
            Project(fields, subquery) => {
                subquery.execute(&db)?.project(fields)
            },
            Select(condition, subquery) => {
                let fd = db.function_dict();
                subquery.execute(&db)?.filter(&fd, condition)
            },
            _ => unimplemented!()
        }
    }
}

#[derive(Debug, Clone)]
pub enum Condition {
    Value(Value),
    QueryField(QueryField),
    FunctionCall(FunctionCall),
}
impl Condition {
    pub(crate) fn test(&self,
        function_dict: &HashMap<FunctionName, Function>,
        resolve: &Fn(&QueryField) -> Result<Value, QueryError>,
    ) -> Result<bool, QueryError> {
        match self {
            Condition::Value(v) => match v {
                Value::Boolean(b) => Ok(*b),
                _ => Err(QueryError::TypeError(TypeError::NotBoolean))
            },
            Condition::QueryField(qf) => {
                Condition::Value(resolve(qf)?).test(function_dict, resolve)
            },
            Condition::FunctionCall(fc) => {
                let value = (*fc).resolve_args(&|qf: &QueryField| {
                    resolve(qf)
                })?.apply(function_dict)?;
                Condition::Value(value).test(function_dict, resolve)
            },
        }
    }
}


#[derive(Debug, Clone)]
pub struct QueryField {
    pub table: Option<TableName>,
    pub field: FieldName
}
impl QueryField {
    pub fn new(field: FieldName) -> Self {
        Self { table: None, field }
    }

    pub fn from_table(self, table: TableName) -> Self {
        Self { table: Some(table), ..self }
    }

    /// Is local to the current query
    pub fn is_local(&self) -> bool {
        self.table.is_none()
    }
}
impl fmt::Display for QueryField {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(table) = self.table.clone() {
            write!(f, "{}.{}", table, self.field)
        }
        else {
            write!(f, "{}", self.field)
        }
    }
}

#[derive(Debug)]
pub struct QueryResult {
    fields: Vec<QueryField>,
    rows: Vec<Row>
}
impl QueryResult {
    pub fn field_names(&self) -> Vec<FieldName> {
        self.fields.iter().map(|f| f.field.clone()).collect()
    }

    pub fn rows(&self) -> Vec<Row> {
        self.rows.clone()
    }

    pub(super) fn new(fields: Vec<QueryField>, rows: Vec<Row>) -> Self {
        Self { fields, rows }
    }

    pub(super) fn from_db_table(db: &DataDB, table_name: TableName) -> Result<Self, QueryError> {
        if let Some(table) = db.table(table_name.clone()) {
            let fields = table.fields().iter()
                .map(|f| QueryField::new(f.name()).from_table(table_name.clone()))
                .collect();

            Ok(Self { fields, rows: db.all_rows(table_name).unwrap() })
        }
        else {
            Err(QueryError::NoSuchTable(table_name))
        }
    }

    pub(crate) fn match_field(&self, qf: &QueryField) -> Vec<usize> {
        let mut result = Vec::new();
        for (i, field) in self.fields.iter().enumerate() {
            if field.field == qf.field {
                if qf.table == None || field.table == qf.table {
                    result.push(i);
                }
            }
        }
        result
    }

    pub(crate) fn project(&self, fields: &Vec<QueryField>) -> Result<QueryResult, QueryError> {
        let mut result_fields: Vec<QueryField> = Vec::new();
        let mut result_columns: Vec<usize> = Vec::new();

        for field in fields {
            let matching = self.match_field(&field);
            if matching.is_empty() {
                return Err(QueryError::NoSuchField(field.clone()));
            }
            if matching.len() > 1 {
                return Err(QueryError::AmbiguousField(field.clone()));
            }

            let m = matching[0];
            result_fields.push(field.clone());
            result_columns.push(m);
        }

        Ok(QueryResult {
            fields: result_fields,
            rows: self.rows.iter().map(|row| row.pick_columns(&result_columns)).collect()
        })
    }

    pub(crate) fn filter(&self, function_dict: &HashMap<FunctionName, Function>, condition: &Condition) -> Result<QueryResult, QueryError> {

        let mut rows: Vec<Row> = Vec::new();
        for row in self.rows.clone() {
            let ok = condition.test(function_dict, &|qf: &QueryField| {
                let matching = self.match_field(&qf);
                if matching.is_empty() {
                    return Err(QueryError::NoSuchField(qf.clone()));
                }
                if matching.len() > 1 {
                    return Err(QueryError::AmbiguousField(qf.clone()));
                }

                Ok(row.values()[matching[0]].clone())
            })?;
            if ok {
                rows.push(row);
            }
        }

        Ok(QueryResult {
            fields: self.fields.clone(),
            rows,
        })
    }
}
