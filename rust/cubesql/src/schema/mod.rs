use std::sync::Arc;

use async_trait::async_trait;

use cubeclient::apis::{configuration::Configuration, default_api as cube_api};
use cubeclient::models::{
    V1CubeMeta, V1CubeMetaDimension, V1CubeMetaMeasure, V1CubeMetaSegment, V1LoadRequest,
    V1LoadRequestQuery, V1LoadResponse,
};
use msql_srv::ColumnType;

use crate::compile::TenantContext;
use crate::mysql::AuthContext;
use crate::CubeError;

pub mod ctx;

#[async_trait]
pub trait SchemaService: Send + Sync {
    async fn get_ctx_for_tenant(&self, ctx: &AuthContext) -> Result<TenantContext, CubeError>;

    async fn request(
        &self,
        query: V1LoadRequestQuery,
        ctx: &AuthContext,
    ) -> Result<V1LoadResponse, CubeError>;
}

pub struct SchemaServiceDefaultImpl;

impl SchemaServiceDefaultImpl {
    fn get_client_config_for_ctx(&self, ctx: &AuthContext) -> Configuration {
        let mut cube_config = Configuration::default();
        cube_config.bearer_access_token = Some(ctx.access_token.clone());
        cube_config.base_path = ctx.base_path.clone();

        cube_config
    }
}

crate::di_service!(SchemaServiceDefaultImpl, [SchemaService]);

#[async_trait]
impl SchemaService for SchemaServiceDefaultImpl {
    async fn get_ctx_for_tenant(&self, ctx: &AuthContext) -> Result<TenantContext, CubeError> {
        let response = cube_api::meta_v1(&self.get_client_config_for_ctx(ctx)).await?;

        let ctx = if let Some(cubes) = response.cubes {
            TenantContext { cubes }
        } else {
            TenantContext { cubes: vec![] }
        };

        Ok(ctx)
    }

    async fn request(
        &self,
        query: V1LoadRequestQuery,
        ctx: &AuthContext,
    ) -> Result<V1LoadResponse, CubeError> {
        let request = V1LoadRequest {
            query: Some(query),
            query_type: Some("multi".to_string()),
        };
        let response =
            cube_api::load_v1(&self.get_client_config_for_ctx(ctx), Some(request)).await?;

        Ok(response)
    }
}

pub trait V1CubeMetaMeasureExt {
    fn get_real_name(&self) -> String;

    fn is_same_agg_type(&self, expect_agg_type: &String) -> bool;

    fn get_mysql_type(&self) -> ColumnType;

    fn mysql_type_as_str(&self) -> String;
}

impl V1CubeMetaMeasureExt for V1CubeMetaMeasure {
    fn get_real_name(&self) -> String {
        let (_, dimension_name) = self.name.split_once('.').unwrap();

        dimension_name.to_string()
    }

    fn is_same_agg_type(&self, expect_agg_type: &String) -> bool {
        if self.agg_type.is_some() {
            if expect_agg_type.eq(&"countDistinct".to_string()) {
                let agg_type = self.agg_type.as_ref().unwrap();

                agg_type.eq(&"countDistinct".to_string())
                    || agg_type.eq(&"countDistinctApprox".to_string())
            } else {
                self.agg_type.as_ref().unwrap().eq(expect_agg_type)
            }
        } else {
            false
        }
    }

    fn get_mysql_type(&self) -> ColumnType {
        let from_type = match &self._type.to_lowercase().as_str() {
            &"number" => ColumnType::MYSQL_TYPE_DOUBLE,
            &"boolean" => ColumnType::MYSQL_TYPE_TINY,
            _ => ColumnType::MYSQL_TYPE_STRING,
        };

        match &self.agg_type {
            Some(agg_type) => match agg_type.as_str() {
                "count" => ColumnType::MYSQL_TYPE_LONGLONG,
                _ => from_type,
            },
            _ => from_type,
        }
    }

    fn mysql_type_as_str(&self) -> String {
        match self._type.to_lowercase().as_str() {
            _ => "int".to_string(),
        }
    }
}

pub trait V1CubeMetaSegmentExt {
    fn get_real_name(&self) -> String;
}

impl V1CubeMetaSegmentExt for V1CubeMetaSegment {
    fn get_real_name(&self) -> String {
        let (_, segment_name) = self.name.split_once('.').unwrap();

        segment_name.to_string()
    }
}

pub trait V1CubeMetaDimensionExt {
    fn get_real_name(&self) -> String;

    fn mysql_can_be_null(&self) -> bool;

    fn mysql_type_as_str(&self) -> String;
}

impl V1CubeMetaDimensionExt for V1CubeMetaDimension {
    fn get_real_name(&self) -> String {
        let (_, dimension_name) = self.name.split_once('.').unwrap();

        dimension_name.to_string()
    }

    fn mysql_can_be_null(&self) -> bool {
        // @todo Possible not null?
        true
    }

    fn mysql_type_as_str(&self) -> String {
        match self._type.to_lowercase().as_str() {
            "time" => "datetime".to_string(),
            _ => "varchar(255)".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct CubeColumn {
    name: String,
    ty: String,
    can_be_null: bool,
}

impl CubeColumn {
    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn mysql_type_as_str(&self) -> &String {
        &self.ty
    }

    pub fn mysql_can_be_null(&self) -> bool {
        self.can_be_null
    }
}

pub trait V1CubeMetaExt {
    fn get_columns(&self) -> Vec<CubeColumn>;
}

impl V1CubeMetaExt for V1CubeMeta {
    fn get_columns(&self) -> Vec<CubeColumn> {
        let mut columns = Vec::new();

        for measure in &self.measures {
            columns.push(CubeColumn {
                name: measure.get_real_name(),
                ty: measure.mysql_type_as_str(),
                can_be_null: false,
            });
        }

        for dimension in &self.dimensions {
            columns.push(CubeColumn {
                name: dimension.get_real_name(),
                ty: dimension.mysql_type_as_str(),
                can_be_null: dimension.mysql_can_be_null(),
            });
        }

        for segment in &self.segments {
            columns.push(CubeColumn {
                name: segment.get_real_name(),
                ty: "boolean".to_string(),
                can_be_null: false,
            });
        }

        columns
    }
}
