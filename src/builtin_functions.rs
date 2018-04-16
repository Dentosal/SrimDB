// use reduce::Reduce;

use Value;
use QueryError;
use function::NativeFunction;

fn f_strict_eq(values: Vec<Value>) -> Result<Value, QueryError> {
    if values.len() < 2 {
        return Ok(Value::Boolean(true));
    }

    let reference = values[0].clone();
    for value in values.iter().skip(1) {
        if *value != reference {
            return Ok(Value::Boolean(false));
        }
    }

    Ok(Value::Boolean(true))
}

fn f_add(values: Vec<Value>) -> Result<Value, QueryError> {
    if values.len() < 1 {
        return Err(QueryError::NotEnoughArguments(1));
    }

    let mut acc = values[0].clone();
    for value in values.iter().skip(1) {
        acc = acc.binop_add(value.clone())?;
    }
    Ok(acc)
}


pub const FUNCTIONS: [(&'static str, NativeFunction); 2] = [
    ("strict_eq", NativeFunction::new(&f_strict_eq)),
    ("add", NativeFunction::new(&f_add)),
];
