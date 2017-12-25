extern crate meval;

fn trim_parenthesis(arg: &str) -> &str {
    arg.trim_left_matches('(').trim_right_matches(')')
}

pub fn maybe_trim_parenthesis(arg: &str) -> &str{
    if arg.starts_with("(") && arg.ends_with(")"){
        trim_parenthesis(arg)
    }else{
        arg
    }
}

pub fn eval_f64(expr: &str) -> Result<f64,meval::Error> {
    meval::eval_str(expr)
}
