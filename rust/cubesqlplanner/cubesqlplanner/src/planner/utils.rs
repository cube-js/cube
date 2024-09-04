//FIXME may be should call function form JS BaseQuery
pub fn escape_column_name(column_name: &String) -> String {
    format!("\"{}\"", column_name)
}
