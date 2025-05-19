use crate::plan::FilterItem;
use crate::planner::sql_evaluator::MemberSymbol;
use std::rc::Rc;

#[derive(Clone, Debug)]
pub struct PrettyPrintConfig {
    ident_space: usize,
}

impl Default for PrettyPrintConfig {
    fn default() -> Self {
        Self { ident_space: 2 }
    }
}

#[derive(Clone, Debug)]
pub struct PrettyPrintState {
    config: PrettyPrintConfig,
    level: usize,
    indent: String,
}

pub struct PrettyPrintResult {
    result: Vec<String>,
}

impl PrettyPrintResult {
    pub fn new() -> Self {
        Self { result: Vec::new() }
    }

    pub fn println(&mut self, text: &str, state: &PrettyPrintState) {
        self.result.push(state.format(text));
    }

    pub fn into_string(self) -> String {
        self.result.join("\n")
    }
}

impl PrettyPrintState {
    pub fn new(config: PrettyPrintConfig, level: usize) -> Self {
        Self {
            level,
            indent: " ".repeat(level * config.ident_space),
            config,
        }
    }

    pub fn new_level(&self) -> Self {
        Self::new(self.config.clone(), self.level + 1)
    }

    pub fn format(&self, text: &str) -> String {
        format!("{}{}", self.indent, text)
    }
}

impl Default for PrettyPrintState {
    fn default() -> Self {
        Self::new(PrettyPrintConfig::default(), 0)
    }
}

pub trait PrettyPrint {
    fn pretty_print(&self, result: &mut PrettyPrintResult, state: &PrettyPrintState);
}

pub fn print_symbol(symbol: &MemberSymbol) -> String {
    format!("{}", symbol.full_name())
}

pub fn print_symbols(symbols: &[Rc<MemberSymbol>]) -> String {
    symbols
        .iter()
        .map(|s| print_symbol(s))
        .collect::<Vec<_>>()
        .join(", ")
}

pub fn pretty_print<T: PrettyPrint>(obj: &T) -> String {
    let mut result = PrettyPrintResult::new();
    let state = PrettyPrintState::default();
    obj.pretty_print(&mut result, &state);
    result.into_string()
}

pub fn pretty_print_rc<T: PrettyPrint>(obj: &Rc<T>) -> String {
    let mut result = PrettyPrintResult::new();
    let state = PrettyPrintState::default();
    obj.pretty_print(&mut result, &state);
    result.into_string()
}

pub fn pretty_print_filter_item(
    result: &mut PrettyPrintResult,
    state: &PrettyPrintState,
    filter_item: &FilterItem,
) {
    match filter_item {
        FilterItem::Item(item) => {
            result.println(
                &format!(
                    "{{name: {}, operator: {} values: [{}]}}",
                    item.member_name(),
                    item.filter_operator().to_string(),
                    item.values()
                        .iter()
                        .map(|v| v.clone().unwrap_or("null".to_string()))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                state,
            );
        }
        FilterItem::Group(group) => {
            result.println(&format!("{{{}:[", group.operator.to_string()), state);
            for item in group.items.iter() {
                pretty_print_filter_item(result, state, item);
            }
            result.println("]}", state);
        }
        FilterItem::Segment(base_segment) => {
            result.println(&format!("{{segment: {}}}", base_segment.full_name()), state);
        }
    }
}
