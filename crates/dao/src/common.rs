
fn is_keyword(s: &str) -> bool {
    let keywords = ["user", "role"];
    keywords.contains(&s)
}

pub fn keywords_safe(s: &str) -> String {
    if is_keyword(s){
        format!("\"{}\"", s)
    }
    else{
        s.to_string()
    }
}
