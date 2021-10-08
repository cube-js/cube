use log::{error, warn};
use msql_srv::ColumnType;

use crate::compile::builder::CompiledQueryFieldMeta;

#[derive(Clone, Debug)]
pub struct Column {
    name: String,
    column_type: ColumnType,
}

impl Column {
    pub fn new(name: String, column_type: ColumnType) -> Column {
        Column { name, column_type }
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_type(&self) -> ColumnType {
        self.column_type
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    values: Vec<TableValue>,
}

impl Row {
    pub fn new(values: Vec<TableValue>) -> Row {
        Row { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn values(&self) -> &Vec<TableValue> {
        &self.values
    }

    pub fn hydrate_from_response(
        meta: &Vec<CompiledQueryFieldMeta>,
        record: &serde_json::Map<String, serde_json::Value>,
    ) -> Row {
        let mut values: Vec<TableValue> = vec![];

        for column_meta in meta {
            let value = record.get(column_meta.column_from.as_str()).unwrap();

            match column_meta.column_type {
                ColumnType::MYSQL_TYPE_LONG | ColumnType::MYSQL_TYPE_LONGLONG => {
                    let column_value = match value {
                        serde_json::Value::Null => TableValue::Null,
                        serde_json::Value::Number(number) => match number.as_i64() {
                            Some(v) => TableValue::Int64(v),
                            None => TableValue::Null,
                        },
                        serde_json::Value::String(s) => match s.parse::<i64>() {
                            Ok(v) => TableValue::Int64(v),
                            Err(error) => {
                                warn!("Unable to parse value as i64: {}", error.to_string());

                                TableValue::Null
                            }
                        },
                        v => {
                            error!(
                                "Unable to map value {:?} to MYSQL_TYPE_LONG (returning null)",
                                v
                            );

                            TableValue::Null
                        }
                    };

                    values.push(column_value);
                }
                ColumnType::MYSQL_TYPE_DOUBLE => {
                    let column_value = match value {
                        serde_json::Value::Null => TableValue::Null,
                        serde_json::Value::Number(number) => match number.as_f64() {
                            Some(v) => TableValue::Float64(v),
                            None => TableValue::Null,
                        },
                        serde_json::Value::String(s) => match s.parse::<f64>() {
                            Ok(v) => TableValue::Float64(v),
                            Err(error) => {
                                warn!("Unable to parse value as f64: {}", error.to_string());

                                TableValue::Null
                            }
                        },
                        v => {
                            error!(
                                "Unable to map value {:?} to MYSQL_TYPE_DOUBLE (returning null)",
                                v
                            );

                            TableValue::Null
                        }
                    };

                    values.push(column_value);
                }
                ColumnType::MYSQL_TYPE_STRING => {
                    if let Some(v) = value.as_str() {
                        values.push(TableValue::String(v.to_string()))
                    } else {
                        values.push(TableValue::Null);
                    }
                }
                ColumnType::MYSQL_TYPE_TINY => {
                    let column_value = match value {
                        serde_json::Value::Null => TableValue::Null,
                        serde_json::Value::Bool(v) => TableValue::Boolean(*v),
                        v => {
                            error!(
                                "Unable to map value {:?} to MYSQL_TYPE_TINY (boolean) (returning null)",
                                v
                            );

                            TableValue::Null
                        }
                    };

                    values.push(column_value);
                }
                _ => panic!(
                    "Unsupported column_type in hydration: {:?}",
                    column_meta.column_type
                ),
            }
        }

        Self::new(values)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TableValue {
    Null,
    String(String),
    Int64(i64),
    Boolean(bool),
    Float64(f64),
}

pub struct DataFrame {
    columns: Vec<Column>,
    data: Vec<Row>,
}

impl DataFrame {
    pub fn new(columns: Vec<Column>, data: Vec<Row>) -> DataFrame {
        DataFrame { columns, data }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn get_rows(&self) -> &Vec<Row> {
        &self.data
    }

    pub fn mut_rows(&mut self) -> &mut Vec<Row> {
        &mut self.data
    }

    pub fn into_rows(self) -> Vec<Row> {
        self.data
    }
}

#[cfg(test)]
mod tests {
    use serde_json;

    use super::*;

    #[test]
    fn test_hydrate_from_response() {
        let meta = vec![
            CompiledQueryFieldMeta {
                column_from: "KibanaSampleDataEcommerce.count".to_string(),
                column_to: "count".to_string(),
                column_type: ColumnType::MYSQL_TYPE_LONGLONG,
            },
            CompiledQueryFieldMeta {
                column_from: "KibanaSampleDataEcommerce.maxPrice".to_string(),
                column_to: "maxPrice".to_string(),
                column_type: ColumnType::MYSQL_TYPE_DOUBLE,
            },
            CompiledQueryFieldMeta {
                column_from: "KibanaSampleDataEcommerce.isBool".to_string(),
                column_to: "isBool".to_string(),
                column_type: ColumnType::MYSQL_TYPE_TINY,
            },
        ];

        let response = r#"
            [
                {"KibanaSampleDataEcommerce.count": null, "KibanaSampleDataEcommerce.maxPrice": null, "KibanaSampleDataEcommerce.isBool": null},
                {"KibanaSampleDataEcommerce.count": 5, "KibanaSampleDataEcommerce.maxPrice": 5.05, "KibanaSampleDataEcommerce.isBool": true},
                {"KibanaSampleDataEcommerce.count": "5", "KibanaSampleDataEcommerce.maxPrice": "5.05", "KibanaSampleDataEcommerce.isBool": false}
            ]
        "#;
        let data = serde_json::from_str::<Vec<serde_json::Value>>(&response).unwrap();
        assert_eq!(
            Row::hydrate_from_response(&meta, data[0].as_object().unwrap()),
            Row::new(vec![TableValue::Null, TableValue::Null, TableValue::Null])
        );
        assert_eq!(
            Row::hydrate_from_response(&meta, data[1].as_object().unwrap()),
            Row::new(vec![
                TableValue::Int64(5),
                TableValue::Float64(5.05),
                TableValue::Boolean(true)
            ])
        );
        assert_eq!(
            Row::hydrate_from_response(&meta, data[2].as_object().unwrap()),
            Row::new(vec![
                TableValue::Int64(5),
                TableValue::Float64(5.05),
                TableValue::Boolean(false)
            ])
        );
    }
}
