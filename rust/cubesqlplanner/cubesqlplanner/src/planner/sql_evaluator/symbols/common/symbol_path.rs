use std::rc::Rc;

use cubenativeutils::CubeError;

use crate::cube_bridge::evaluator::CubeEvaluator;

pub enum SymbolPathType {
    Dimension,
    Measure,
}

pub struct SymbolPath {
    pub path_type: SymbolPathType,
    pub path: Vec<String>,
    pub symbol_name: String,
    pub granularity: Option<String>,
}

impl SymbolPath {
    pub fn parse(cube_evaluator: Rc<dyn CubeEvaluator>, path: &str) -> Result<Self, CubeError> {
        let parts = path.split(".").collect::<Vec<&str>>();
        if parts.len() < 2 {
            return Err(CubeError::user(format!("Invalid symbol path: {}", path)));
        }

        if let Some(dim_path) =
            Self::try_parse_as_dimension_with_granularity(cube_evaluator.clone(), &parts)?
        {
            return Ok(dim_path);
        }

        let path_to_check = vec![
            parts[parts.len() - 2].to_string(),
            parts[parts.len() - 1].to_string(),
        ];

        let path_type = if cube_evaluator.is_dimension(path_to_check.clone())? {
            SymbolPathType::Dimension
        } else if cube_evaluator.is_measure(path_to_check.clone())? {
            SymbolPathType::Measure
        } else {
            return Err(CubeError::user(format!(
                "Symbol path doesn't refer to a dimension or measure: {}",
                path
            )));
        };

        let path = parts[0..parts.len() - 2]
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        let symbol_name = path_to_check.join(".");

        return Ok(Self {
            path_type,
            path,
            symbol_name,
            granularity: None,
        });
    }

    fn try_parse_as_dimension_with_granularity(
        cube_evaluator: Rc<dyn CubeEvaluator>,
        parts: &[&str],
    ) -> Result<Option<Self>, CubeError> {
        if parts.len() > 2 {
            let path_to_check = vec![
                parts[parts.len() - 3].to_string(),
                parts[parts.len() - 2].to_string(),
            ];
            if cube_evaluator.is_dimension(path_to_check.clone())? {
                let path_type = SymbolPathType::Dimension;
                let path = parts[0..parts.len() - 3]
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect();
                let symbol_name = path_to_check.join(".");
                let granularity = Some(parts[parts.len() - 1].to_string());
                return Ok(Some(Self {
                    path_type,
                    path,
                    symbol_name,
                    granularity,
                }));
            }
        }
        Ok(None)
    }
}
