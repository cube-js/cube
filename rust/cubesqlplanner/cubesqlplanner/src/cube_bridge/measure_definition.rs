use super::memeber_sql::{MemberSql, NativeMemberSql};
use cubenativeutils::wrappers::serializer::{
    NativeDeserialize, NativeDeserializer, NativeSerialize,
};
use cubenativeutils::wrappers::NativeContextHolder;
use cubenativeutils::wrappers::NativeObjectHandle;
use cubenativeutils::CubeError;
use serde::{Deserialize, Serialize};
use std::rc::Rc;

#[derive(Serialize, Deserialize, Debug)]
pub struct MeasureDefinitionStatic {
    #[serde(rename = "type")]
    pub measure_type: String,
    pub owned_by_cube: Option<bool>,
}

#[nativebridge::native_bridge(MeasureDefinitionStatic)]
pub trait MeasureDefinition {
    #[optional]
    #[field]
    fn sql(&self) -> Result<Option<Rc<dyn MemberSql>>, CubeError>;
}

/*
export type MeasureDefinition = {
  type: string,
  sql: Function,
  ownedByCube: boolean,
  rollingWindow?: any
  filters?: any
  primaryKey?: true,
  drillFilters?: any,
  postAggregate?: boolean,
  groupBy?: Function,
  reduceBy?: Function,
  addGroupBy?: Function,
  timeShift?: TimeShiftDefinition[],
  groupByReferences?: string[],
  reduceByReferences?: string[],
  addGroupByReferences?: string[],
  timeShiftReferences?: TimeShiftDefinitionReference[],
};
 */
