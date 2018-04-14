use TableName;
use FieldName;
use Row;
use QueryError;
use DataDB;

#[derive(Debug)]
pub enum Query {
    /// Multiset Union
    Table(TableName),

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
            Project(fields, subquery) => {
                subquery.execute(&db)?.project(fields)
            }
            _ => unimplemented!()
        }
    }
}

#[derive(Debug)]
pub enum Condition {
    True,
    False,
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

    pub(crate) fn from_db_table(db: &DataDB, table_name: TableName) -> Result<Self, QueryError> {
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
                if qf.table == None || field.table == qf.table{
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
}

/*

Companies
id  name            city
0   TuubaSoft       Redmond
1   Microsoft       Redmond
2   Google          Mountain View
3   Oracle          Redwood City
4   Apple           Silicon Valley

Employees
id  name            company
0   Alice           TuubaSoft
1   John            TuubaSoft
2   Jack            Microsoft
3   Rose            Microsoft
4   Will            Apple
5   Steve           Apple
6   Bobby           Apple


# Find out first names of all people working for Redmond companies

SELECT Employees.name
FROM (
    SELECT Employees.name, Companies.city
    FROM Employees
    JOIN Companies
        ON Companies.name == Employees.company
    WHERE Companies.city == "Redmond"
)

JOIN Employees Companies
ON eq(Employees::company, Companies::name)

*/
