use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, ArrayRef, StringBuilder, UInt32Builder, UInt64Builder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::{datasource::TableProviderFilterPushDown, TableProvider, TableType},
    error::DataFusionError,
    logical_plan::Expr,
    physical_plan::{memory::MemoryExec, ExecutionPlan},
};

struct InformationSchemaCollationsBuilder {
    collation_names: StringBuilder,
    character_set_names: StringBuilder,
    ids: UInt64Builder,
    is_defaults: StringBuilder,
    is_compiled_values: StringBuilder,
    sortlens: UInt32Builder,
    pad_attributes: StringBuilder,
}

impl InformationSchemaCollationsBuilder {
    fn new() -> Self {
        let capacity = 10;

        Self {
            collation_names: StringBuilder::new(capacity),
            character_set_names: StringBuilder::new(capacity),
            ids: UInt64Builder::new(capacity),
            is_defaults: StringBuilder::new(capacity),
            is_compiled_values: StringBuilder::new(capacity),
            sortlens: UInt32Builder::new(capacity),
            pad_attributes: StringBuilder::new(capacity),
        }
    }

    fn add_collation(
        &mut self,
        collation_name: impl AsRef<str>,
        character_set_name: impl AsRef<str>,
        id: u64,
        is_default: bool,
        sortlen: u32,
        no_pad: bool,
    ) {
        self.collation_names
            .append_value(collation_name.as_ref())
            .unwrap();
        self.character_set_names
            .append_value(character_set_name.as_ref())
            .unwrap();
        self.ids.append_value(id).unwrap();
        self.is_defaults
            .append_value((if is_default { "Yes" } else { "" }).to_string())
            .unwrap();
        self.is_compiled_values
            .append_value("Yes".to_string())
            .unwrap();
        self.sortlens.append_value(sortlen).unwrap();
        self.pad_attributes
            .append_value((if no_pad { "NO PAD" } else { "PAD SPACE" }).to_string())
            .unwrap();
    }

    fn finish(mut self) -> Vec<Arc<dyn Array>> {
        let mut columns: Vec<Arc<dyn Array>> = vec![];

        columns.push(Arc::new(self.collation_names.finish()));
        columns.push(Arc::new(self.character_set_names.finish()));
        columns.push(Arc::new(self.ids.finish()));
        columns.push(Arc::new(self.is_defaults.finish()));
        columns.push(Arc::new(self.is_compiled_values.finish()));
        columns.push(Arc::new(self.sortlens.finish()));
        columns.push(Arc::new(self.pad_attributes.finish()));

        columns
    }
}

pub struct InfoSchemaCollationsProvider {
    data: Arc<Vec<ArrayRef>>,
}

impl InfoSchemaCollationsProvider {
    pub fn new() -> Self {
        let mut builder = InformationSchemaCollationsBuilder::new();

        builder.add_collation("utf8mb4_general_ci", "utf8mb4", 45, false, 1, false);
        builder.add_collation("utf8mb4_bin", "utf8mb4", 46, false, 1, false);
        builder.add_collation("utf8mb4_unicode_ci", "utf8mb4", 224, false, 8, false);
        builder.add_collation("utf8mb4_icelandic_ci", "utf8mb4", 225, false, 8, false);
        builder.add_collation("utf8mb4_latvian_ci", "utf8mb4", 226, false, 8, false);
        builder.add_collation("utf8mb4_romanian_ci", "utf8mb4", 227, false, 8, false);
        builder.add_collation("utf8mb4_slovenian_ci", "utf8mb4", 228, false, 8, false);
        builder.add_collation("utf8mb4_polish_ci", "utf8mb4", 229, false, 8, false);
        builder.add_collation("utf8mb4_estonian_ci", "utf8mb4", 230, false, 8, false);
        builder.add_collation("utf8mb4_spanish_ci", "utf8mb4", 231, false, 8, false);
        builder.add_collation("utf8mb4_swedish_ci", "utf8mb4", 232, false, 8, false);
        builder.add_collation("utf8mb4_turkish_ci", "utf8mb4", 233, false, 8, false);
        builder.add_collation("utf8mb4_czech_ci", "utf8mb4", 234, false, 8, false);
        builder.add_collation("utf8mb4_danish_ci", "utf8mb4", 235, false, 8, false);
        builder.add_collation("utf8mb4_lithuanian_ci", "utf8mb4", 236, false, 8, false);
        builder.add_collation("utf8mb4_slovak_ci", "utf8mb4", 237, false, 8, false);
        builder.add_collation("utf8mb4_spanish2_ci", "utf8mb4", 238, false, 8, false);
        builder.add_collation("utf8mb4_roman_ci", "utf8mb4", 239, false, 8, false);
        builder.add_collation("utf8mb4_persian_ci", "utf8mb4", 240, false, 8, false);
        builder.add_collation("utf8mb4_esperanto_ci", "utf8mb4", 241, false, 8, false);
        builder.add_collation("utf8mb4_hungarian_ci", "utf8mb4", 242, false, 8, false);
        builder.add_collation("utf8mb4_sinhala_ci", "utf8mb4", 243, false, 8, false);
        builder.add_collation("utf8mb4_german2_ci", "utf8mb4", 244, false, 8, false);
        builder.add_collation("utf8mb4_croatian_ci", "utf8mb4", 245, false, 8, false);
        builder.add_collation("utf8mb4_unicode_520_ci", "utf8mb4", 246, false, 8, false);
        builder.add_collation("utf8mb4_vietnamese_ci", "utf8mb4", 247, false, 8, false);
        builder.add_collation("utf8mb4_0900_ai_ci", "utf8mb4", 255, true, 0, true);
        builder.add_collation("utf8mb4_de_pb_0900_ai_ci", "utf8mb4", 256, false, 0, true);
        builder.add_collation("utf8mb4_is_0900_ai_ci", "utf8mb4", 257, false, 0, true);
        builder.add_collation("utf8mb4_lv_0900_ai_ci", "utf8mb4", 258, false, 0, true);
        builder.add_collation("utf8mb4_ro_0900_ai_ci", "utf8mb4", 259, false, 0, true);
        builder.add_collation("utf8mb4_sl_0900_ai_ci", "utf8mb4", 260, false, 0, true);
        builder.add_collation("utf8mb4_pl_0900_ai_ci", "utf8mb4", 261, false, 0, true);
        builder.add_collation("utf8mb4_et_0900_ai_ci", "utf8mb4", 262, false, 0, true);
        builder.add_collation("utf8mb4_es_0900_ai_ci", "utf8mb4", 263, false, 0, true);
        builder.add_collation("utf8mb4_sv_0900_ai_ci", "utf8mb4", 264, false, 0, true);
        builder.add_collation("utf8mb4_tr_0900_ai_ci", "utf8mb4", 265, false, 0, true);
        builder.add_collation("utf8mb4_cs_0900_ai_ci", "utf8mb4", 266, false, 0, true);
        builder.add_collation("utf8mb4_da_0900_ai_ci", "utf8mb4", 267, false, 0, true);
        builder.add_collation("utf8mb4_lt_0900_ai_ci", "utf8mb4", 268, false, 0, true);
        builder.add_collation("utf8mb4_sk_0900_ai_ci", "utf8mb4", 269, false, 0, true);
        builder.add_collation("utf8mb4_es_trad_0900_ai_ci", "utf8mb4", 270, false, 0, true);
        builder.add_collation("utf8mb4_la_0900_ai_ci", "utf8mb4", 271, false, 0, true);
        builder.add_collation("utf8mb4_eo_0900_ai_ci", "utf8mb4", 273, false, 0, true);
        builder.add_collation("utf8mb4_hu_0900_ai_ci", "utf8mb4", 274, false, 0, true);
        builder.add_collation("utf8mb4_hr_0900_ai_ci", "utf8mb4", 275, false, 0, true);
        builder.add_collation("utf8mb4_vi_0900_ai_ci", "utf8mb4", 277, false, 0, true);
        builder.add_collation("utf8mb4_0900_as_cs", "utf8mb4", 278, false, 0, true);
        builder.add_collation("utf8mb4_de_pb_0900_as_cs", "utf8mb4", 279, false, 0, true);
        builder.add_collation("utf8mb4_is_0900_as_cs", "utf8mb4", 280, false, 0, true);
        builder.add_collation("utf8mb4_lv_0900_as_cs", "utf8mb4", 281, false, 0, true);
        builder.add_collation("utf8mb4_ro_0900_as_cs", "utf8mb4", 282, false, 0, true);
        builder.add_collation("utf8mb4_sl_0900_as_cs", "utf8mb4", 283, false, 0, true);
        builder.add_collation("utf8mb4_pl_0900_as_cs", "utf8mb4", 284, false, 0, true);
        builder.add_collation("utf8mb4_et_0900_as_cs", "utf8mb4", 285, false, 0, true);
        builder.add_collation("utf8mb4_es_0900_as_cs", "utf8mb4", 286, false, 0, true);
        builder.add_collation("utf8mb4_sv_0900_as_cs", "utf8mb4", 287, false, 0, true);
        builder.add_collation("utf8mb4_tr_0900_as_cs", "utf8mb4", 288, false, 0, true);
        builder.add_collation("utf8mb4_cs_0900_as_cs", "utf8mb4", 289, false, 0, true);
        builder.add_collation("utf8mb4_da_0900_as_cs", "utf8mb4", 290, false, 0, true);
        builder.add_collation("utf8mb4_lt_0900_as_cs", "utf8mb4", 291, false, 0, true);
        builder.add_collation("utf8mb4_sk_0900_as_cs", "utf8mb4", 292, false, 0, true);
        builder.add_collation("utf8mb4_es_trad_0900_as_cs", "utf8mb4", 293, false, 0, true);
        builder.add_collation("utf8mb4_la_0900_as_cs", "utf8mb4", 294, false, 0, true);
        builder.add_collation("utf8mb4_eo_0900_as_cs", "utf8mb4", 296, false, 0, true);
        builder.add_collation("utf8mb4_hu_0900_as_cs", "utf8mb4", 297, false, 0, true);
        builder.add_collation("utf8mb4_hr_0900_as_cs", "utf8mb4", 298, false, 0, true);
        builder.add_collation("utf8mb4_vi_0900_as_cs", "utf8mb4", 300, false, 0, true);
        builder.add_collation("utf8mb4_ja_0900_as_cs", "utf8mb4", 303, false, 0, true);
        builder.add_collation("utf8mb4_ja_0900_as_cs_ks", "utf8mb4", 304, false, 24, true);
        builder.add_collation("utf8mb4_0900_as_ci", "utf8mb4", 305, false, 0, true);
        builder.add_collation("utf8mb4_ru_0900_ai_ci", "utf8mb4", 306, false, 0, true);
        builder.add_collation("utf8mb4_ru_0900_as_cs", "utf8mb4", 307, false, 0, true);
        builder.add_collation("utf8mb4_zh_0900_as_cs", "utf8mb4", 308, false, 0, true);
        builder.add_collation("utf8mb4_0900_bin", "utf8mb4", 309, false, 1, true);

        Self {
            data: Arc::new(builder.finish()),
        }
    }
}

#[async_trait]
impl TableProvider for InfoSchemaCollationsProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("COLLATION_NAME", DataType::Utf8, false),
            Field::new("CHARACTER_SET_NAME", DataType::Utf8, false),
            Field::new("ID", DataType::UInt64, false),
            Field::new("IS_DEFAULT", DataType::Utf8, false),
            Field::new("IS_COMPILED", DataType::Utf8, false),
            Field::new("SORTLEN", DataType::UInt32, false),
            Field::new("PAD_ATTRIBUTE", DataType::Utf8, false),
        ]))
    }

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batch = RecordBatch::try_new(self.schema(), self.data.to_vec())?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![batch]],
            self.schema(),
            projection.clone(),
        )?))
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown, DataFusionError> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
