use TableName;
use FieldName;
use FieldKind;
use Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    name: TableName,
    fields: Vec<TableField>,
    key_field_mask: Vec<bool>,
}
impl Table {
    pub fn new(name: &str, fields: Vec<TableField>) -> Self {
        Self {
            name: name.to_owned(),
            key_field_mask: vec![true; fields.clone().len()],
            fields,
        }
    }

    pub fn with_key_fields(mut self, key_field_names: Vec<FieldName>) -> Self {
        let mut mask = vec![false; self.fields.len()];
        for field_name in key_field_names {
            if let Some(i) = self.field_index(field_name.clone()) {
                mask[i] = true;
            }
            else {
                panic!("Field '{}' does not exists in table '{}'", field_name, self.name);
            }
        }

        self.key_field_mask = mask;
        self
    }

    pub fn name(&self) -> TableName {
        self.name.clone()
    }

    pub fn fields(&self) -> Vec<TableField> {
        self.fields.clone()
    }

    pub fn field_index(&self, field_name: FieldName) -> Option<usize> {
        for (i, field) in self.fields.iter().enumerate() {
            if field.name() == field_name {
                return Some(i);
            }
        }
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableField {
    name: FieldName,
    kind: FieldKind
}
impl TableField {
    pub fn new(name: &str, kind: FieldKind) -> Self {
        Self { name: name.to_owned(), kind }
    }

    pub fn name(&self) -> FieldName {
        self.name.clone()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    values: Vec<Value>
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }
    pub fn pick_columns(&self, columns: &Vec<usize>) -> Self {
        Self::new(columns.iter().map(|i| self.values[*i].clone()).collect())
    }
}
