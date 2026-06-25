use super::expression::Expression;

#[derive(Clone)]
pub enum CaseLabel {
    String(String),
    Sql(Expression),
}

/// Predicate-style `case`: ordered `WHEN <sql> THEN <label>` arms.
#[derive(Clone)]
pub struct Case {
    pub when: Vec<CaseWhen>,
    pub else_label: Option<CaseLabel>,
}

#[derive(Clone)]
pub struct CaseWhen {
    pub sql: Expression,
    pub label: CaseLabel,
}

/// Switch-style case: selector + arms keyed by literal value. The arm
/// label and the else label are computed via SQL in the schema, hence
/// `Expression`.
#[derive(Clone)]
pub struct CaseSwitch {
    pub selector: Expression,
    pub when: Vec<CaseSwitchWhen>,
    pub else_label: Option<Expression>,
}

#[derive(Clone)]
pub struct CaseSwitchWhen {
    pub value: String,
    pub label: Expression,
}

#[derive(Clone)]
pub enum CaseVariant {
    Predicate(Case),
    Switch(CaseSwitch),
}
