#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeFieldKind {
    Field,
    Call,
    Static,
}

impl BridgeFieldKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            BridgeFieldKind::Field => "field",
            BridgeFieldKind::Call => "call",
            BridgeFieldKind::Static => "static",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct BridgeFieldMeta {
    pub name: &'static str,
    pub js_name: &'static str,
    pub kind: BridgeFieldKind,
    pub optional: bool,
    pub vec: bool,
}
