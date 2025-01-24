use std::{
    num::{ParseFloatError, ParseIntError},
    str::ParseBoolError,
};

// `from_op` is generated as if-else chain from op.parse()
// Because of this a lot of instances of FromStr::Err will be constructed just as a "take other branch" marker
// This type should be very cheap to construct, at least while we use FromStr chains
// Don't store any input in here, FromStr is not designed for this, and it requires allocation
// Instead rely on egg::FromOpError for now, it will return allocated `op`, but only once in the end
// TODO make parser dispatch stricter, and return both input and LanguageParseError from `from_op`
#[derive(thiserror::Error, Debug)]
pub enum LanguageParseError {
    #[error("Should start with '{0}'")]
    ShouldStartWith(&'static str),
    #[error("Can't be matched against {0}")]
    ShouldMatch(&'static str),
    #[error("Should contain a valid type")]
    InvalidType,
    #[error("Should contains a valid scalar type")]
    InvalidScalarType,
    #[error("Can't parse boolean scalar value with error: {0}")]
    InvalidBoolValue(#[source] ParseBoolError),
    #[error("Can't parse i64 scalar value with error: {0}")]
    InvalidIntValue(#[source] ParseIntError),
    #[error("Can't parse f64 scalar value with error: {0}")]
    InvalidFloatValue(#[source] ParseFloatError),
    #[error("Conversion from string is not supported")]
    NotSupported,
}

#[macro_export]
macro_rules! plan_to_language {
    ($(#[$meta:meta])* $vis:vis enum $name:ident $variants:tt) => {
        $crate::__plan_to_language!($(#[$meta])* $vis enum $name $variants -> {});
    };
}

#[macro_export]
macro_rules! variant_field_struct {
    ($variant:ident, $var_field:ident, String) => {
        paste::item! {
            #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
            pub struct [<$variant $var_field:camel>](String);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    const PREFIX: &'static str = concat!(std::stringify!([<$variant $var_field:camel>]), ":");
                    if let Some(suffix) =  s.strip_prefix(PREFIX) {
                        return Ok([<$variant $var_field:camel>](suffix.to_string()));
                    }
                    Err(Self::Err::ShouldStartWith(PREFIX))
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}", self.0)
                }
            }
        }
    };

    ($variant:ident, $var_field:ident, usize) => {
        paste::item! {
            #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
            pub struct [<$variant $var_field:camel>](usize);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    const PREFIX: &'static str = concat!(std::stringify!([<$variant $var_field:camel>]), ":");
                    if let Some(suffix) =  s.strip_prefix(PREFIX) {
                        return Ok([<$variant $var_field:camel>](suffix.parse().unwrap()));
                    }
                    Err(Self::Err::ShouldStartWith(PREFIX))
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{:?}", self.0)
                }
            }
        }
    };

    ($variant:ident, $var_field:ident, bool) => {
        paste::item! {
            #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
            pub struct [<$variant $var_field:camel>](bool);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    const PREFIX: &'static str = concat!(std::stringify!([<$variant $var_field:camel>]), ":");
                    if let Some(suffix) = s.strip_prefix(PREFIX) {
                        return Ok([<$variant $var_field:camel>](suffix.parse().unwrap()));
                    }
                    Err(Self::Err::ShouldStartWith(PREFIX))
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{:?}", self.0)
                }
            }
        }
    };

    ($variant:ident, $var_field:ident, Option<usize>) => {
        paste::item! {
            #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
            pub struct [<$variant $var_field:camel>](Option<usize>);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    const PREFIX: &'static str = concat!(std::stringify!([<$variant $var_field:camel>]), ":");
                    if let Some(suffix) = s.strip_prefix(PREFIX) {
                        if suffix == "None" {
                            return Ok([<$variant $var_field:camel>](None));
                        } else {
                            return Ok([<$variant $var_field:camel>](Some(suffix.parse().unwrap())));
                        }
                    }
                    Err(Self::Err::ShouldStartWith(PREFIX))
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{:?}", self.0)
                }
            }
        }
    };

    ($variant:ident, $var_field:ident, Option<Vec<String>>) => {
        paste::item! {
            #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
            pub struct [<$variant $var_field:camel>](Option<Vec<String>>);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    const PREFIX: &'static str = concat!(std::stringify!([<$variant $var_field:camel>]), ":");
                    if let Some(suffix) = s.strip_prefix(PREFIX) {
                        if suffix == "None" {
                            return Ok([<$variant $var_field:camel>](None));
                        } else {
                            return Ok([<$variant $var_field:camel>](Some(suffix.split(',').map(|s| s.to_string()).collect::<Vec<_>>())));
                        }
                    }
                    Err(Self::Err::ShouldStartWith(PREFIX))
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{:?}", self.0)
                }
            }
        }
    };

    ($variant:ident, $var_field:ident, Option<String>) => {
        paste::item! {
            #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
            pub struct [<$variant $var_field:camel>](Option<String>);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    const PREFIX: &'static str = concat!(std::stringify!([<$variant $var_field:camel>]), ":");
                    if let Some(suffix) = s.strip_prefix(PREFIX) {
                        if suffix == "None" {
                            return Ok([<$variant $var_field:camel>](None));
                        } else {
                            return Ok([<$variant $var_field:camel>](Some(suffix.to_string())));
                        }
                    }
                    Err(Self::Err::NotSupported)
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{:?}", self.0)
                }
            }
        }
    };

    ($variant:ident, $var_field:ident, Column) => {
        paste::item! {
            #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
            pub struct [<$variant $var_field:camel>](Column);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(_s: &str) -> Result<Self, Self::Err> {
                    Err(Self::Err::NotSupported)
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}", self.0)
                }
            }
        }
    };

    ($variant:ident, $var_field:ident, Vec<Column>) => {
        paste::item! {
            #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
            pub struct [<$variant $var_field:camel>](Vec<Column>);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(_s: &str) -> Result<Self, Self::Err> {
                    Err(Self::Err::NotSupported)
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{:?}", self.0)
                }
            }
        }
    };

    ($variant:ident, $var_field:ident, Vec<JoinType>) => {
        paste::item! {
            #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
            pub struct [<$variant $var_field:camel>](Vec<Column>);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(_s: &str) -> Result<Self, Self::Err> {
                    Err(Self::Err::NotSupported)
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{:?}", self.0)
                }
            }
        }
    };

    ($variant:ident, $var_field:ident, Arc<AggregateUDF>) => {
        paste::item! {
            #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
            pub struct [<$variant $var_field:camel>](String);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    const PREFIX: &'static str = concat!(std::stringify!([<$variant $var_field:camel>]), ":");
                    if let Some(suffix) =  s.strip_prefix(PREFIX) {
                        return Ok([<$variant $var_field:camel>](suffix.to_string()));
                    }
                    Err(Self::Err::ShouldStartWith(PREFIX))
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}", self.0)
                }
            }
        }
    };

    ($variant:ident, $var_field:ident, Arc<TableUDF>) => {
        paste::item! {
            #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
            pub struct [<$variant $var_field:camel>](String);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(_s: &str) -> Result<Self, Self::Err> {
                    Err(Self::Err::NotSupported)
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}", self.0)
                }
            }
        }
    };

    ($variant:ident, $var_field:ident, Arc<ScalarUDF>) => {
        paste::item! {
            #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
            pub struct [<$variant $var_field:camel>](String);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    const PREFIX: &'static str = concat!(std::stringify!([<$variant $var_field:camel>]), ":");
                    if let Some(suffix) = s.strip_prefix(PREFIX) {
                        return Ok([<$variant $var_field:camel>](suffix.to_string()));
                    }
                    Err(Self::Err::ShouldStartWith(PREFIX))
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}", self.0)
                }
            }
        }
    };

    ($variant:ident, $var_field:ident, AggregateFunction) => {
        $crate::variant_field_struct!(
            @enum_struct $variant, $var_field, { AggregateFunction } -> {
                AggregateFunction::Count => "Count",
                AggregateFunction::Sum => "Sum",
                AggregateFunction::Min => "Min",
                AggregateFunction::Max => "Max",
                AggregateFunction::Avg => "Avg",
                AggregateFunction::ApproxDistinct => "ApproxDistinct",
                AggregateFunction::ArrayAgg => "ArrayAgg",
                AggregateFunction::Variance => "Variance",
                AggregateFunction::VariancePop => "VariancePop",
                AggregateFunction::Stddev => "Stddev",
                AggregateFunction::StddevPop => "StddevPop",
                AggregateFunction::Covariance => "Covariance",
                AggregateFunction::CovariancePop => "CovariancePop",
                AggregateFunction::Correlation => "Correlation",
                AggregateFunction::ApproxPercentileCont => "ApproxPercentileCont",
                AggregateFunction::ApproxPercentileContWithWeight => "ApproxPercentileContWithWeight",
                AggregateFunction::ApproxMedian => "ApproxMedian",
                AggregateFunction::BoolAnd => "BoolAnd",
                AggregateFunction::BoolOr => "BoolOr",
            }
        );
    };

    ($variant:ident, $var_field:ident, BuiltinScalarFunction) => {
        $crate::variant_field_struct!(
            @enum_struct $variant, $var_field, { BuiltinScalarFunction } -> {
                BuiltinScalarFunction::Abs => "Abs",
                BuiltinScalarFunction::Acos => "Acos",
                BuiltinScalarFunction::Asin => "Asin",
                BuiltinScalarFunction::Atan => "Atan",
                BuiltinScalarFunction::Ceil => "Ceil",
                BuiltinScalarFunction::Cos => "Cos",
                BuiltinScalarFunction::Digest => "Digest",
                BuiltinScalarFunction::Exp => "Exp",
                BuiltinScalarFunction::Floor => "Floor",
                BuiltinScalarFunction::Ln => "Ln",
                BuiltinScalarFunction::Log => "Log",
                BuiltinScalarFunction::Log10 => "Log10",
                BuiltinScalarFunction::Log2 => "Log2",
                BuiltinScalarFunction::Round => "Round",
                BuiltinScalarFunction::Signum => "Signum",
                BuiltinScalarFunction::Sin => "Sin",
                BuiltinScalarFunction::Sqrt => "Sqrt",
                BuiltinScalarFunction::Tan => "Tan",
                BuiltinScalarFunction::Trunc => "Trunc",
                BuiltinScalarFunction::MakeArray => "MakeArray",
                BuiltinScalarFunction::Ascii => "Ascii",
                BuiltinScalarFunction::BitLength => "BitLength",
                BuiltinScalarFunction::Btrim => "Btrim",
                BuiltinScalarFunction::CharacterLength => "CharacterLength",
                BuiltinScalarFunction::Chr => "Chr",
                BuiltinScalarFunction::Concat => "Concat",
                BuiltinScalarFunction::ConcatWithSeparator => "ConcatWithSeparator",
                BuiltinScalarFunction::DatePart => "DatePart",
                BuiltinScalarFunction::DateTrunc => "DateTrunc",
                BuiltinScalarFunction::InitCap => "InitCap",
                BuiltinScalarFunction::Left => "Left",
                BuiltinScalarFunction::Lpad => "Lpad",
                BuiltinScalarFunction::Lower => "Lower",
                BuiltinScalarFunction::Ltrim => "Ltrim",
                BuiltinScalarFunction::MD5 => "MD5",
                BuiltinScalarFunction::NullIf => "NullIf",
                BuiltinScalarFunction::OctetLength => "OctetLength",
                BuiltinScalarFunction::Pi => "Pi",
                BuiltinScalarFunction::Random => "Random",
                BuiltinScalarFunction::RegexpReplace => "RegexpReplace",
                BuiltinScalarFunction::Repeat => "Repeat",
                BuiltinScalarFunction::Replace => "Replace",
                BuiltinScalarFunction::Reverse => "Reverse",
                BuiltinScalarFunction::Right => "Right",
                BuiltinScalarFunction::Rpad => "Rpad",
                BuiltinScalarFunction::Rtrim => "Rtrim",
                BuiltinScalarFunction::SHA224 => "SHA224",
                BuiltinScalarFunction::SHA256 => "SHA256",
                BuiltinScalarFunction::SHA384 => "SHA384",
                BuiltinScalarFunction::SHA512 => "SHA512",
                BuiltinScalarFunction::SplitPart => "SplitPart",
                BuiltinScalarFunction::StartsWith => "StartsWith",
                BuiltinScalarFunction::Strpos => "Strpos",
                BuiltinScalarFunction::Substr => "Substr",
                BuiltinScalarFunction::ToHex => "ToHex",
                BuiltinScalarFunction::ToTimestamp => "ToTimestamp",
                BuiltinScalarFunction::ToTimestampMillis => "ToTimestampMillis",
                BuiltinScalarFunction::ToTimestampMicros => "ToTimestampMicros",
                BuiltinScalarFunction::ToTimestampSeconds => "ToTimestampSeconds",
                BuiltinScalarFunction::ToMonthInterval => "ToMonthInterval",
                BuiltinScalarFunction::ToDayInterval => "ToDayInterval",
                BuiltinScalarFunction::Now => "Now",
                BuiltinScalarFunction::UtcTimestamp => "UtcTimestamp",
                BuiltinScalarFunction::CurrentDate => "CurrentDate",
                BuiltinScalarFunction::Translate => "Translate",
                BuiltinScalarFunction::Trim => "Trim",
                BuiltinScalarFunction::Upper => "Upper",
                BuiltinScalarFunction::RegexpMatch => "RegexpMatch",
                BuiltinScalarFunction::Coalesce => "Coalesce",
            }
        );
    };

    ($variant:ident, $var_field:ident, Operator) => {
        $crate::variant_field_struct!(
            @enum_struct $variant, $var_field, { Operator } -> {
                Operator::Eq => "=",
                Operator::NotEq => "!=",
                Operator::Lt => "<",
                Operator::LtEq => "<=",
                Operator::Gt => ">",
                Operator::GtEq => ">=",
                Operator::Plus => "+",
                Operator::Minus => "-",
                Operator::Multiply => "*",
                Operator::Divide => "/",
                Operator::Modulo => "%",
                Operator::Exponentiate => "^",
                Operator::And => "AND",
                Operator::Or => "OR",
                Operator::Like => "LIKE",
                Operator::NotLike => "NOT_LIKE",
                Operator::ILike => "ILIKE",
                Operator::NotILike => "NOT_ILIKE",
                Operator::RegexMatch => "~",
                Operator::RegexIMatch => "~*",
                Operator::RegexNotMatch => "!~",
                Operator::RegexNotIMatch => "!~*",
                Operator::IsDistinctFrom => "IS_DISTINCT_FROM",
                Operator::IsNotDistinctFrom => "IS_NOT_DISTINCT_FROM",
                Operator::BitwiseAnd => "&",
                Operator::BitwiseOr => "|",
                Operator::BitwiseShiftRight => ">>",
                Operator::BitwiseShiftLeft => "<<",
                Operator::StringConcat => "||",
            }
        );
    };

    ($variant:ident, $var_field:ident, JoinType) => {
        $crate::variant_field_struct!(
            @enum_struct $variant, $var_field, { JoinType } -> {
                JoinType::Inner => "Inner",
                JoinType::Left => "Left",
                JoinType::Right => "Right",
                JoinType::Full => "Full",
                JoinType::Semi => "Semi",
                JoinType::Anti => "Anti",
            }
        );
    };

    ($variant:ident, $var_field:ident, JoinConstraint) => {
        $crate::variant_field_struct!(
            @enum_struct $variant, $var_field, { JoinConstraint } -> {
                JoinConstraint::On => "On",
                JoinConstraint::Using => "Using",
            }
        );
    };

    ($variant:ident, $var_field:ident, LikeType) => {
        $crate::variant_field_struct!(
            @enum_struct $variant, $var_field, { LikeType } -> {
                LikeType::Like => "Like",
                LikeType::ILike => "ILike",
                LikeType::SimilarTo => "SimilarTo",
            }
        );
    };

    ($variant:ident, $var_field:ident, WrappedSelectType) => {
        $crate::variant_field_struct!(
            @enum_struct $variant, $var_field, { WrappedSelectType } -> {
                WrappedSelectType::Projection => "Projection",
                WrappedSelectType::Aggregate => "Aggregate",
            }
        );
    };

    ($variant:ident, $var_field:ident, GroupingSetType) => {
        $crate::variant_field_struct!(
            @enum_struct $variant, $var_field, { GroupingSetType } -> {
                GroupingSetType::Rollup => "Rollup",
                GroupingSetType::Cube => "Cube",
            }
        );
    };

    (@enum_struct $variant:ident, $var_field:ident, { $var_field_type:ty } -> {$($variant_type:ty => $name:literal,)*}) => {
        paste::item! {
            #[derive(Debug, Clone)]
            pub struct [<$variant $var_field:camel>]($var_field_type);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    const PREFIX: &'static str = concat!(std::stringify!([<$variant $var_field:camel>]), ":");
                    let name = s
                        .strip_prefix(PREFIX)
                        .ok_or(Self::Err::ShouldStartWith(PREFIX))?;

                    match name {
                        $($name => Ok([<$variant $var_field:camel>]($variant_type)),)*
                        _ => Err(Self::Err::ShouldMatch(std::stringify!($var_field_type)))
                    }
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    let name = match self.0 {
                        $($variant_type => $name,)*
                    };
                    write!(f, "{}", name)
                }
            }

            impl core::cmp::Ord for [<$variant $var_field:camel>] {
                fn cmp(&self, other: &Self) -> core::cmp::Ordering {
                    let name = match self.0 {
                        $($variant_type => $name,)*
                    };
                    let other_name = match other.0 {
                        $($variant_type => $name,)*
                    };
                    name.cmp(other_name)
                }
            }

            impl core::cmp::PartialOrd for [<$variant $var_field:camel>] {
                fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
                    let name = match self.0 {
                        $($variant_type => $name,)*
                    };
                    let other_name = match other.0 {
                        $($variant_type => $name,)*
                    };
                    name.partial_cmp(other_name)
                }
            }

            impl core::hash::Hash for [<$variant $var_field:camel>] {
                fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
                    std::mem::discriminant(&self.0).hash(state);
                }
            }

            impl core::cmp::PartialEq for [<$variant $var_field:camel>] {
                fn eq(&self, other: &[<$variant $var_field:camel>]) -> bool {
                    let name = match self.0 {
                        $($variant_type => $name,)*
                    };
                    let other_name = match other.0 {
                        $($variant_type => $name,)*
                    };
                    name == other_name
                }
            }

            impl core::cmp::Eq for [<$variant $var_field:camel>] {}
        }
    };

    ($variant:ident, $var_field:ident, DataType) => {
        paste::item! {
            #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
            pub struct [<$variant $var_field:camel>](DataType);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    const PREFIX: &'static str = concat!(std::stringify!([<$variant $var_field:camel>]), ":");
                    let typed_str = s
                        .strip_prefix(PREFIX)
                        .ok_or(Self::Err::ShouldStartWith(PREFIX))?;

                    match typed_str {
                        "Float32" => Ok([<$variant $var_field:camel>](DataType::Float32)),
                        "Float64" => Ok([<$variant $var_field:camel>](DataType::Float64)),
                        "Int32" => Ok([<$variant $var_field:camel>](DataType::Int32)),
                        "Int64" => Ok([<$variant $var_field:camel>](DataType::Int64)),
                        "Boolean" => Ok([<$variant $var_field:camel>](DataType::Boolean)),
                        "Utf8" => Ok([<$variant $var_field:camel>](DataType::Utf8)),
                        "Date32" => Ok([<$variant $var_field:camel>](DataType::Date32)),
                        "Date64" => Ok([<$variant $var_field:camel>](DataType::Date64)),
                        _ => Err(Self::Err::InvalidType),
                    }
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{}", self.0)
                }
            }
        }
    };

    ($variant:ident, $var_field:ident, ScalarValue) => {
        paste::item! {
            #[derive(Debug, PartialOrd, Clone)]
            pub struct [<$variant $var_field:camel>](ScalarValue);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(s: &str) -> Result<Self, Self::Err> {
                    const PREFIX: &'static str = concat!(std::stringify!([<$variant $var_field:camel>]), ":");
                    let typed_str = s
                        .strip_prefix(PREFIX)
                        .ok_or(Self::Err::ShouldStartWith(PREFIX))?;

                    if let Some(value) = typed_str.strip_prefix("s:") {
                        Ok([<$variant $var_field:camel>](ScalarValue::Utf8(Some(value.to_string()))))
                    } else if let Some(value) = typed_str.strip_prefix("b:") {
                        let n: bool = value.parse().map_err(|err| Self::Err::InvalidBoolValue(err))?;
                        Ok([<$variant $var_field:camel>](ScalarValue::Boolean(Some(n))))
                    } else if let Some(value) = typed_str.strip_prefix("i:") {
                        let n: i64 = value.parse().map_err(|err| Self::Err::InvalidIntValue(err))?;
                        Ok([<$variant $var_field:camel>](ScalarValue::Int64(Some(n))))
                    } else if let Some(value) = typed_str.strip_prefix("f:") {
                        let n: f64 = value.parse().map_err(|err| Self::Err::InvalidFloatValue(err))?;
                        Ok([<$variant $var_field:camel>](ScalarValue::Float64(Some(n))))
                    } else if typed_str == "null" {
                        Ok([<$variant $var_field:camel>](ScalarValue::Null))
                    } else {
                        Err(Self::Err::InvalidScalarType)
                    }
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{:?}", self)
                }
            }

            impl core::cmp::Ord for [<$variant $var_field:camel>] {
                fn cmp(&self, other: &Self) -> core::cmp::Ordering {
                    self.partial_cmp(&other).unwrap()
                }
            }

            impl core::hash::Hash for [<$variant $var_field:camel>] {
                fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
                    self.0.hash(state);
                }
            }

            impl core::cmp::PartialEq for [<$variant $var_field:camel>] {
                fn eq(&self, other: &[<$variant $var_field:camel>]) -> bool {
                    // TODO Datafusion has incorrect Timestamp comparison without timezone involved
                    match &self.0 {
                        ScalarValue::TimestampNanosecond(_, self_tz) => {
                            match &other.0 {
                                ScalarValue::TimestampNanosecond(_, other_tz) => {
                                    self_tz == other_tz && self.0 == other.0
                                }
                                _ => self.0 == other.0
                            }
                        }
                        _ => self.0 == other.0
                    }
                }
            }

            impl core::cmp::Eq for [<$variant $var_field:camel>] {}
        }
    };

    ($variant:ident, $var_field:ident, $var_field_type:ty) => {
        paste::item! {
            #[derive(Debug, PartialOrd, Clone)]
            pub struct [<$variant $var_field:camel>]($var_field_type);

            impl FromStr for [<$variant $var_field:camel>] {
                type Err = $crate::compile::rewrite::language::LanguageParseError;
                fn from_str(_s: &str) -> Result<Self, Self::Err> {
                    Err(Self::Err::NotSupported)
                }
            }

            impl std::fmt::Display for [<$variant $var_field:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, "{:?}", self)
                }
            }

            impl core::cmp::Ord for [<$variant $var_field:camel>] {
                fn cmp(&self, other: &Self) -> core::cmp::Ordering {
                    self.partial_cmp(&other).unwrap()
                }
            }

            impl core::hash::Hash for [<$variant $var_field:camel>] {
                fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
                    self.0.hash(state);
                }
            }

            impl core::cmp::PartialEq for [<$variant $var_field:camel>] {
                fn eq(&self, other: &[<$variant $var_field:camel>]) -> bool {
                    self.0 == other.0
                }
            }

            impl core::cmp::Eq for [<$variant $var_field:camel>] {}
        }
    };
}

#[macro_export]
macro_rules! __plan_to_language {
    (@define_language $(#[$meta:meta])* $vis:vis enum $name:ident {} ->
     $decl:tt {$($matches:tt)*} $children:tt $children_mut:tt
     $display:tt {$($from_op:tt)*} {$($type_decl:tt)*}
    ) => { paste::item! {
        $(#[$meta])*
        #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
        $vis enum $name $decl

        $($type_decl)*

        impl egg::Language for $name {
            type Discriminant = std::mem::Discriminant<Self>;

            #[inline(always)]
            fn matches(&self, other: &Self) -> bool {
                ::std::mem::discriminant(self) == ::std::mem::discriminant(other) &&
                match (self, other) { $($matches)* _ => false }
            }

            fn children(&self) -> &[egg::Id] { match self $children }
            fn children_mut(&mut self) -> &mut [egg::Id] { match self $children_mut }
            fn discriminant(&self) -> Self::Discriminant { std::mem::discriminant(self) }
        }

        impl ::std::fmt::Display for $name {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                // We need to pass `f` to the match expression for hygiene
                // reasons.
                match (self, f) $display
            }
        }

        impl egg::FromOp for $name {
            type Error = egg::FromOpError;

            fn from_op(op: &str, children: ::std::vec::Vec<egg::Id>) -> ::std::result::Result<Self, Self::Error> {
                match (op, children) {
                    $($from_op)*
                    (op, children) => Err(egg::FromOpError::new(op, children)),
                }
            }
        }
    }};

    (@define_language $(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident ($ids:ty),
         $($variants:tt)*
     } ->
     { $($decl:tt)* } { $($matches:tt)* } { $($children:tt)* } { $($children_mut:tt)* }
     { $($display:tt)* } { $($from_op:tt)* } { $($type_decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            @define_language
            $(#[$meta])* $vis enum $name
            { $($variants)* } ->
            { $($decl)*          $variant($ids), }
            { $($matches)*       ($name::$variant(l), $name::$variant(r)) => egg::LanguageChildren::len(l) == egg::LanguageChildren::len(r), }
            { $($children)*      $name::$variant(ids) => egg::LanguageChildren::as_slice(ids), }
            { $($children_mut)*  $name::$variant(ids) => egg::LanguageChildren::as_mut_slice(ids), }
            { $($display)*       ($name::$variant(..), f) => f.write_str(std::stringify!($variant)), }
            { $($from_op)*       (op, children) if op == std::stringify!($variant) && <$ids as egg::LanguageChildren>::can_be_length(children.len()) => {
                  let children = <$ids as egg::LanguageChildren>::from_vec(children);
                  Ok($name::$variant(children))
              },
            }
            { $($type_decl)* }
        );
    };

    (@define_language $(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident $var_field:ident ($ids:ty),
         $($variants:tt)*
     } ->
     { $($decl:tt)* } { $($matches:tt)* } { $($children:tt)* } { $($children_mut:tt)* }
     { $($display:tt)* } { $($from_op:tt)* } { $($type_decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            @define_language
            $(#[$meta])* $vis enum $name
            { $($variants)* } ->
            { $($decl)*          [<$variant $var_field:camel>]($ids), }
            { $($matches)*       ($name::[<$variant $var_field:camel>](l), $name::[<$variant $var_field:camel>](r)) => egg::LanguageChildren::len(l) == egg::LanguageChildren::len(r), }
            { $($children)*      $name::[<$variant $var_field:camel>](ids) => egg::LanguageChildren::as_slice(ids), }
            { $($children_mut)*  $name::[<$variant $var_field:camel>](ids) => egg::LanguageChildren::as_mut_slice(ids), }
            { $($display)*       ($name::[<$variant $var_field:camel>](..), f) => f.write_str(std::stringify!([<$variant $var_field:camel>])), }
            { $($from_op)*       (op, children) if op == std::stringify!([<$variant $var_field:camel>]) && <$ids as egg::LanguageChildren>::can_be_length(children.len()) => {
                  let children = <$ids as egg::LanguageChildren>::from_vec(children);
                  Ok($name::[<$variant $var_field:camel>](children))
              },
            }
            { $($type_decl)* }
        );
    };

    (@define_language $(#[$meta:meta])* $vis:vis enum $name:ident
     {
         @data $variant:ident $var_field:ident ($($data:tt)*),
         $($variants:tt)*
     } ->
     { $($decl:tt)* } { $($matches:tt)* } { $($children:tt)* } { $($children_mut:tt)* }
     { $($display:tt)* } { $($from_op:tt)* } { $($type_decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            @define_language $(#[$meta])* $vis enum $name
            { $($variants)* } ->
            { $($decl)*          [<$variant $var_field:camel>]([<$variant $var_field:camel>]), }
            { $($matches)*       ($name::[<$variant $var_field:camel>](data1), $name::[<$variant $var_field:camel>](data2)) => data1 == data2, }
            { $($children)*      $name::[<$variant $var_field:camel>](_data) => &[], }
            { $($children_mut)*  $name::[<$variant $var_field:camel>](_data) => &mut [], }
            { $($display)*       ($name::[<$variant $var_field:camel>](data), f) => ::std::fmt::Display::fmt(data, f), }
            { $($from_op)*       (op, children) if op.parse::<[<$variant $var_field:camel>]>().is_ok() && children.is_empty() => Ok($name::[<$variant $var_field:camel>](op.parse().unwrap())), }
            {
                $($type_decl)*
                $crate::variant_field_struct!($variant, $var_field, $($data)*);
            }
        );
    };

    // Here transform from variants to @define_language begins.
    // It transforms variant fields to language variants.
    // The reason it's so complex and not part of @define_language is we can't call macros inside
    // enum declaration block, i.e. we can't do { $($decl)* $(enum_decl!($var_field, $var_field_type),)* }.

    ($(#[$meta:meta])* $vis:vis enum $name:ident {} ->
     $decl:tt
    ) => {
        $crate::__plan_to_language! {
            @define_language
            $(#[$meta])*
            $vis enum $name $decl
            -> {} {} {} {} {} {} {}
        }
    };

    ($(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident {
            @variant_size $variant_size:expr,
         },
         $($variants:tt)*
     } ->
     { $($decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            $(#[$meta])* $vis enum $name
            { $($variants)* } ->
            { $($decl)* $variant([egg::Id; $variant_size]), }
        );
    };

    // Reference rules

    ($(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident {
            @variant_size $variant_size:expr,
            $var_field:ident : Arc<LogicalPlan>,
            $($var_fields:tt)*
         },
         $($variants:tt)*
     } ->
     { $($decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            $(#[$meta])* $vis enum $name
            {
                $variant {
                    @variant_size $variant_size + 1,
                    $($var_fields)*
                },
                $($variants)*
            } ->
            { $($decl)* }
        );
    };

    ($(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident {
            @variant_size $variant_size:expr,
            $var_field:ident : Expr,
            $($var_fields:tt)*
         },
         $($variants:tt)*
     } ->
     { $($decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            $(#[$meta])* $vis enum $name
            {
                $variant {
                    @variant_size $variant_size + 1,
                    $($var_fields)*
                },
                $($variants)*
            } ->
            { $($decl)* }
        );
    };

    ($(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident {
            @variant_size $variant_size:expr,
            $var_field:ident : Arc<Expr>,
            $($var_fields:tt)*
         },
         $($variants:tt)*
     } ->
     { $($decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            $(#[$meta])* $vis enum $name
            {
                $variant {
                    @variant_size $variant_size + 1,
                    $($var_fields)*
                },
                $($variants)*
            } ->
            { $($decl)* }
        );
    };

    ($(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident {
            @variant_size $variant_size:expr,
            $var_field:ident : Box<Expr>,
            $($var_fields:tt)*
         },
         $($variants:tt)*
     } ->
     { $($decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            $(#[$meta])* $vis enum $name
            {
                $variant {
                    @variant_size $variant_size + 1,
                    $($var_fields)*
                },
                $($variants)*
            } ->
            { $($decl)* }
        );
    };

    // References inside container

    ($(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident {
            @variant_size $variant_size:expr,
            $var_field:ident : Vec<LogicalPlan>,
            $($var_fields:tt)*
         },
         $($variants:tt)*
     } ->
     { $($decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            $(#[$meta])* $vis enum $name
            {
                $variant {
                    @variant_size $variant_size + 1,
                    $($var_fields)*
                },
                $($variants)*
            } ->
            { $($decl)* $variant $var_field (Vec<egg::Id>), }
        );
    };

    ($(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident {
            @variant_size $variant_size:expr,
            $var_field:ident : Vec<Expr>,
            $($var_fields:tt)*
         },
         $($variants:tt)*
     } ->
     { $($decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            $(#[$meta])* $vis enum $name
            {
                $variant {
                    @variant_size $variant_size + 1,
                    $($var_fields)*
                },
                $($variants)*
            } ->
            { $($decl)* $variant $var_field (Vec<egg::Id>), }
        );
    };

    ($(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident {
            @variant_size $variant_size:expr,
            $var_field:ident : Vec<(Box<Expr>, Box<Expr>)>,
            $($var_fields:tt)*
         },
         $($variants:tt)*
     } ->
     { $($decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            $(#[$meta])* $vis enum $name
            {
                $variant {
                    @variant_size $variant_size + 1,
                    $($var_fields)*
                },
                $($variants)*
            } ->
            { $($decl)* $variant $var_field (Vec<egg::Id>), }
        );
    };

    ($(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident {
            @variant_size $variant_size:expr,
            $var_field:ident : Option<Box<Expr>>,
            $($var_fields:tt)*
         },
         $($variants:tt)*
     } ->
     { $($decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            $(#[$meta])* $vis enum $name
            {
                $variant {
                    @variant_size $variant_size + 1,
                    $($var_fields)*
                },
                $($variants)*
            } ->
            { $($decl)* $variant $var_field (Vec<egg::Id>), }
        );
    };

    // Skip schema as it isn't part of rewrite. TODO remove?

    ($(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident {
            @variant_size $variant_size:expr,
            $var_field:ident : DFSchemaRef,
            $($var_fields:tt)*
         },
         $($variants:tt)*
     } ->
     { $($decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            $(#[$meta])* $vis enum $name
            {
                $variant {
                    @variant_size $variant_size,
                    $($var_fields)*
                },
                $($variants)*
            } ->
            { $($decl)* }
        );
    };

    ($(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident {
            @variant_size $variant_size:expr,
            $var_field:ident : @collect_var_field_type ($($var_field_type:tt)*),
            $($var_fields:tt)*
         },
         $($variants:tt)*
     } ->
     { $($decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            $(#[$meta])* $vis enum $name
            {
                $variant {
                    @variant_size $variant_size + 1,
                    $($var_fields)*
                },
                $($variants)*
            } ->
            { $($decl)* @data $variant $var_field ($($var_field_type)*), }
        );
    };

    ($(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident {
            @variant_size $variant_size:expr,
            $var_field:ident : @collect_var_field_type ($($var_field_type:tt)*) $token:tt $($var_fields:tt)*
         },
         $($variants:tt)*
     } ->
     { $($decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            $(#[$meta])* $vis enum $name
            {
                $variant {
                    @variant_size $variant_size,
                    $var_field : @collect_var_field_type ($($var_field_type)* $token) $($var_fields)*
                },
                $($variants)*
            } ->
            { $($decl)* }
        );
    };

    ($(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident {
            @variant_size $variant_size:expr,
            $var_field:ident : $($var_fields:tt)*
         },
         $($variants:tt)*
     } ->
     { $($decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            $(#[$meta])* $vis enum $name
            {
                $variant {
                    @variant_size $variant_size,
                    $var_field : @collect_var_field_type () $($var_fields)*
                },
                $($variants)*
            } ->
            { $($decl)* }
        );
    };

    ($(#[$meta:meta])* $vis:vis enum $name:ident
     {
         $variant:ident {
            $($var_fields:tt)*
         },
         $($variants:tt)*
     } ->
     { $($decl:tt)* }
    ) => {
        $crate::__plan_to_language!(
            $(#[$meta])* $vis enum $name
            {
                $variant {
                    @variant_size 0usize,
                    $($var_fields)*
                },
                $($variants)*
            } ->
            { $($decl)* }
        );
    };
}
