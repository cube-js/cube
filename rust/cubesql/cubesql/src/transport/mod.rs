pub(crate) mod ctx;
pub(crate) mod cubestore_transport;
pub(crate) mod ext;
pub(crate) mod hybrid_transport;
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
// Format
pub type CubeMetaSimpleFormat = cubeclient::models::V1CubeMetaSimpleFormat;
pub type CubeMetaCustomNumericFormat = cubeclient::models::V1CubeMetaCustomNumericFormat;
pub type CubeMetaCustomNumericFormatType = cubeclient::models::V1CubeMetaCustomNumericFormatType;
pub type CubeMetaCustomTimeFormat = cubeclient::models::V1CubeMetaCustomTimeFormat;
pub type CubeMetaCustomTimeFormatType = cubeclient::models::V1CubeMetaCustomTimeFormatType;
pub type CubeMetaLinkFormat = cubeclient::models::V1CubeMetaLinkFormat;
pub type CubeMetaLinkFormatType = cubeclient::models::V1CubeMetaLinkFormatType;
pub type CubeMetaDimensionOrder = cubeclient::models::V1CubeMetaDimensionOrder;

pub type CubeMetaFormat = cubeclient::models::V1CubeMetaFormat;
// Request/Response
pub type TransportLoadResponse = cubeclient::models::V1LoadResponse;
pub type TransportLoadRequestQuery = cubeclient::models::V1LoadRequestQuery;
pub type TransportLoadRequest = cubeclient::models::V1LoadRequest;
pub type TransportLoadRequestCacheMode = cubeclient::models::Cache;
pub type TransportMetaResponse = cubeclient::models::V1MetaResponse;
pub type TransportError = cubeclient::models::V1Error;

pub use ctx::*;
pub use cubestore_transport::*;
pub use ext::*;
pub use hybrid_transport::*;
pub use service::*;
