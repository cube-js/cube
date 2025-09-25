pub(crate) mod ctx;
pub(crate) mod ext;
pub(crate) mod service;

// Re-export types to minimise version maintenance for crate users such as cloud
pub type CubeMeta = cubeclient::models::V1CubeMeta;
pub type CubeMetaType = cubeclient::models::V1CubeMetaType;
pub type CubeMetaDimension = cubeclient::models::V1CubeMetaDimension;
pub type CubeMetaMeasure = cubeclient::models::V1CubeMetaMeasure;
pub type CubeMetaSegment = cubeclient::models::V1CubeMetaSegment;
pub type CubeMetaJoin = cubeclient::models::V1CubeMetaJoin;
pub type CubeMetaFolder = cubeclient::models::V1CubeMetaFolder;
pub type CubeMetaNestedFolder = cubeclient::models::V1CubeMetaNestedFolder;
pub type CubeMetaNestedFolderMember = cubeclient::models::V1CubeMetaNestedFolderMember;
pub type CubeMetaHierarchy = cubeclient::models::V1CubeMetaHierarchy;
pub type CubeMetaDimensionFormat = cubeclient::models::V1CubeMetaDimensionFormat;
pub type CubeMetaDimensionSimpleFormat = cubeclient::models::V1CubeMetaDimensionSimpleFormat;
pub type CubeMetaDimensionLinkFormat = cubeclient::models::V1CubeMetaDimensionLinkFormat;
pub type CubeMetaDimensionLinkFormatType = cubeclient::models::V1CubeMetaDimensionLinkFormatType;
pub type CubeMetaMeasureFormat = cubeclient::models::V1CubeMetaMeasureFormat;
// Request/Response
pub type TransportLoadResponse = cubeclient::models::V1LoadResponse;
pub type TransportLoadRequestQuery = cubeclient::models::V1LoadRequestQuery;
pub type TransportLoadRequest = cubeclient::models::V1LoadRequest;
pub type TransportLoadRequestCacheMode = cubeclient::models::Cache;
pub type TransportMetaResponse = cubeclient::models::V1MetaResponse;
pub type TransportError = cubeclient::models::V1Error;

pub use ctx::*;
pub use ext::*;
pub use service::*;
