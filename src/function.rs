use std::collections::HashMap;

use FunctionName;
use QueryField;
use QueryError;
use Value;

#[derive(Clone)]
pub enum Function {
    Native(NativeFunction),
    Composed(FunctionCall),
}
impl Function {
    pub fn call(&self, arguments: Vec<Value>) -> Result<Value, QueryError> {
        match self {
            Function::Native(nf) => nf.call(arguments),
            Function::Composed(cf) => {
                println!("{:?}", cf);
                unimplemented!();
            }
        }
    }
}

#[derive(Clone)]
pub struct NativeFunction {
    function: &'static Fn(Vec<Value>) -> Result<Value, QueryError>
}
impl NativeFunction {
    pub(crate) const fn new(function: &'static Fn(Vec<Value>) -> Result<Value, QueryError>) -> Self {
        Self { function }
    }
    fn call(&self, arguments: Vec<Value>) -> Result<Value, QueryError> {
        (self.function)(arguments)
    }
}

#[derive(Debug, Clone)]
pub enum Argument {
    FunctionCall(FunctionCall),
    Value(Value),
    QueryField(QueryField),
}


#[derive(Debug, Clone)]
pub struct FunctionCall {
    pub target: String,
    pub arguments: Vec<Argument>
}
impl FunctionCall {
    pub fn new(target: String, arguments: Vec<Argument>) -> Self {
        Self {
            target,
            arguments
        }
    }

    pub(crate) fn resolve_args(
        &self,
        resolve: &Fn(&QueryField) -> Result<Value, QueryError>
    ) -> Result<FunctionCall, QueryError> {
        let mut new_args = Vec::new();
        for arg in self.arguments.clone() {
            new_args.push(match arg {
                Argument::FunctionCall(fc) => Argument::FunctionCall(fc.resolve_args(resolve)?),
                Argument::Value(v) => Argument::Value(v.clone()),
                Argument::QueryField(qf) => Argument::Value(resolve(&qf)?),
            });
        }
        Ok(FunctionCall::new(self.target.clone(), new_args))
    }

    pub(crate) fn apply(&self, function_dict: &HashMap<FunctionName, Function>) -> Result<Value, QueryError> {
        let mut args: Vec<Value> = Vec::new();

        for arg in self.arguments.clone() {
            args.push(match arg {
                Argument::Value(v) => v.clone(),
                Argument::FunctionCall(fc) => fc.apply(function_dict)?,
                _ => panic!("Applying with unresolved query fields")
            });
        };

        function_dict.get(&self.target).expect(&format!("No function named '{}'", self.target)).call(args)
    }
}
