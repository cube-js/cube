pub fn format_simple_query_results(messages: &[tokio_postgres::SimpleQueryMessage]) -> String {
    let mut columns: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<String>> = Vec::new();

    for msg in messages {
        match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => {
                if columns.is_empty() {
                    columns = (0..row.columns().len())
                        .map(|i| row.columns()[i].name().to_string())
                        .collect();
                }
                let values: Vec<String> = (0..row.columns().len())
                    .map(|i| row.try_get(i).unwrap_or(None).unwrap_or("NULL").to_string())
                    .collect();
                rows.push(values);
            }
            tokio_postgres::SimpleQueryMessage::CommandComplete(_) => {}
            _ => {}
        }
    }

    if columns.is_empty() {
        return "(empty result)".to_string();
    }

    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in &rows {
        for (i, val) in row.iter().enumerate() {
            if val.len() > widths[i] {
                widths[i] = val.len();
            }
        }
    }

    let mut result = String::new();

    let header: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!("{:width$}", c, width = widths[i]))
        .collect();
    result.push_str(&header.join(" | "));
    result.push('\n');

    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(*w)).collect();
    result.push_str(&sep.join("-+-"));
    result.push('\n');

    for row in &rows {
        let formatted: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, v)| format!("{:width$}", v, width = widths[i]))
            .collect();
        result.push_str(&formatted.join(" | "));
        result.push('\n');
    }

    result
}
