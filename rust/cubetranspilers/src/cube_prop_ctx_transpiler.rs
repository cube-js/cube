use indexmap::IndexSet;
use regex::Regex;
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;
use swc_core::atoms::Atom;
use swc_core::common::errors::Handler;
use swc_core::common::{BytePos, Span, SyntaxContext, DUMMY_SP};
use swc_core::ecma::visit::{Visit, VisitMutWith, VisitWith};

use swc_core::{
    ecma::{ast::*, visit::VisitMut},
    plugin::proxies::PluginSourceMapProxy,
};

static CURRENT_CUBE_CONSTANTS: [&str; 2] = ["CUBE", "TABLE"];

static TRANSPILLED_FIELDS_PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    let patterns = [
        r"sql$",
        r"(sqlTable|sql_table)$",
        r"^measures\.[_a-zA-Z][_a-zA-Z0-9]*\.(drillMemberReferences|drillMembers|drill_members)$",
        r"^measures\.[_a-zA-Z][_a-zA-Z0-9]*\.(orderBy|order_by)\.[0-9]+\.sql$",
        r"^measures\.[_a-zA-Z][_a-zA-Z0-9]*\.(timeShift|time_shift)\.[0-9]+\.(timeDimension|time_dimension)$",
        r"^measures\.[_a-zA-Z][_a-zA-Z0-9]*\.(reduceBy|reduce_by|groupBy|group_by|addGroupBy|add_group_by)$",
        r"^dimensions\.[_a-zA-Z][_a-zA-Z0-9]*\.(reduceBy|reduce_by|groupBy|group_by|addGroupBy|add_group_by)$",
        r"^(preAggregations|pre_aggregations)\.[_a-zA-Z][_a-zA-Z0-9]*\.indexes\.[_a-zA-Z][_a-zA-Z0-9]*\.columns$",
        r"^(preAggregations|pre_aggregations)\.[_a-zA-Z][_a-zA-Z0-9]*\.(timeDimensionReference|timeDimension|time_dimension|segments|dimensions|measures|rollups|segmentReferences|dimensionReferences|measureReferences|rollupReferences)$",
        r"^(preAggregations|pre_aggregations)\.[_a-zA-Z][_a-zA-Z0-9]*\.(timeDimensions|time_dimensions)\.\d+\.dimension$",
        r"^(preAggregations|pre_aggregations)\.[_a-zA-Z][_a-zA-Z0-9]*\.(outputColumnTypes|output_column_types)\.\d+\.member$",
        r"^contextMembers$",
        r"^includes$",
        r"^excludes$",
        r"^hierarchies\.[_a-zA-Z][_a-zA-Z0-9]*\.levels$",
        r"^cubes\.[0-9]+\.(joinPath|join_path)$",
        r"^(accessPolicy|access_policy)\.[0-9]+\.(rowLevel|row_level)\.filters\.[0-9]+.*\.member$",
        r"^(accessPolicy|access_policy)\.[0-9]+\.(rowLevel|row_level)\.filters\.[0-9]+.*\.values$",
        r"^(accessPolicy|access_policy)\.[0-9]+\.conditions.[0-9]+\.if$",
    ];
    patterns
        .iter()
        .map(|pat| Regex::new(pat).expect("Invalid regex pattern"))
        .collect()
});

static TRANSPILLED_FIELDS: LazyLock<HashSet<String>> = LazyLock::new(|| {
    let re_extract = Regex::new(r".*?([_a-zA-Z|][_a-zA-Z0-9|]*)([^_a-zA-Z0-9|]*)$").unwrap();
    let mut set = HashSet::new();

    for regex in TRANSPILLED_FIELDS_PATTERNS.iter() {
        let pat_str = regex.as_str();
        if let Some(caps) = re_extract.captures(pat_str) {
            if let Some(m) = caps.get(1) {
                let fields_str = m.as_str();
                for field in fields_str.split('|') {
                    set.insert(field.to_string());
                }
            }
        }
    }
    set
});

pub struct CubePropTransformVisitor<'a> {
    pub(crate) cube_names: HashSet<String>,
    pub(crate) cube_symbols: HashMap<String, HashMap<String, bool>>,
    pub(crate) context_symbols: HashMap<String, String>,
    pub(crate) source_map: Option<PluginSourceMapProxy>,
    handler: &'a Handler,
}

impl<'a> CubePropTransformVisitor<'a> {
    pub fn new(
        cube_names: HashSet<String>,
        cube_symbols: HashMap<String, HashMap<String, bool>>,
        context_symbols: HashMap<String, String>,
        source_map: Option<PluginSourceMapProxy>,
        handler: &'a Handler,
    ) -> Self {
        CubePropTransformVisitor {
            source_map,
            cube_names,
            cube_symbols,
            context_symbols,
            handler,
        }
    }

    fn emit_error(&self, span: Span, message: &str) {
        self.handler
            .struct_span_err(span, &self.format_msg(span, message))
            .emit();
    }

    fn format_msg(&self, span: Span, message: &str) -> String {
        if let Some(ref sm) = self.source_map {
            if let Some(source_file) = sm.source_file.get() {
                let loc = source_file.lookup_line(span.lo()).unwrap_or(0);
                let column = span.lo() - source_file.line_begin_pos(BytePos(loc as u32));
                format!(
                    "{}. Found in {}:{}:{}",
                    message,
                    source_file.name,
                    loc + 1,
                    column.0,
                )
            } else {
                message.to_string()
            }
        } else {
            message.to_string()
        }
    }

    fn resolve_cube(&self, name: &str) -> bool {
        self.cube_names.contains(name)
    }

    fn is_current_cube(&self, name: &str) -> bool {
        CURRENT_CUBE_CONSTANTS.contains(&name)
    }

    fn resolve_symbol(&self, cube_name: &str, name: &str, span: Span) -> bool {
        if name == "USER_CONTEXT" {
            self.emit_error(
                span,
                "Support for USER_CONTEXT was removed, please migrate to SECURITY_CONTEXT",
            );
            return true;
        }

        if self.context_symbols.contains_key(name) {
            return true;
        }

        let key = if self.is_current_cube(name) {
            cube_name
        } else {
            name
        };

        match self.cube_symbols.get(key) {
            Some(_cube) => true,
            None => match self.cube_symbols.get(cube_name) {
                Some(cube) => cube.get(name).copied().unwrap_or(false),
                None => false,
            },
        }
    }

    fn sql_and_references_field_visitor<'b>(
        &'b mut self,
        cube_name: Option<String>,
    ) -> SqlAndReferencesFieldVisitor<'b, 'a>
    where
        'a: 'b,
    {
        SqlAndReferencesFieldVisitor {
            cube_name,
            parent: self,
            path_stack: Vec::new(),
        }
    }

    fn known_identifiers_inject_visitor<'b>(
        &'b mut self,
        field: String,
    ) -> KnownIdentifiersInjectVisitor<'b, 'a>
    where
        'a: 'b,
    {
        KnownIdentifiersInjectVisitor {
            field,
            parent: self,
        }
    }

    /// Converts the property value (Prop::KeyValue) to an arrow function whose parameters
    /// are unique identifiers collected from the source expression.
    fn transform_object_property(&mut self, prop: &mut Prop, resolve: &dyn Fn(&str) -> bool) {
        if let Prop::KeyValue(ref mut kv) = prop {
            if let Some(new_expr) = self.replace_value_with_arrow_function(resolve, &kv.value) {
                kv.value = Box::new(new_expr);
            }
        }
    }

    /// Collects identifiers from the expression and returns ArrowExpr,
    /// where the parameters are the collected identifiers and the body is the original expression.
    fn replace_value_with_arrow_function(
        &mut self,
        resolve: &dyn Fn(&str) -> bool,
        expr: &Expr,
    ) -> Option<Expr> {
        let mut collector = CollectIdentifiersVisitor {
            identifiers: IndexSet::new(),
            resolve,
        };
        expr.visit_with(&mut collector);
        let params: Vec<Pat> = collector
            .identifiers
            .into_iter()
            .map(|(sym, ctxt)| {
                Pat::Ident(BindingIdent {
                    id: Ident::new(sym, DUMMY_SP, ctxt),
                    type_ann: None,
                })
            })
            .collect();
        let body_expr = match expr {
            Expr::Arrow(arrow_expr) => arrow_expr.body.clone(),
            _ => Box::new(BlockStmtOrExpr::Expr(Box::new((*expr).clone()))),
        };
        let arrow = ArrowExpr {
            span: DUMMY_SP,
            params,
            body: body_expr,
            is_async: false,
            is_generator: false,
            type_params: None,
            return_type: None,
            ctxt: SyntaxContext::empty(),
        };
        Some(Expr::Arrow(arrow))
    }
}

impl VisitMut for CubePropTransformVisitor<'_> {
    // Implement necessary visit_mut_* methods for actual custom transform.
    // A comprehensive list of possible visitor methods can be found here:
    // https://rustdoc.swc.rs/swc_ecma_visit/trait.VisitMut.html

    fn visit_mut_call_expr(&mut self, call_expr: &mut CallExpr) {
        if let Callee::Expr(callee) = &call_expr.callee {
            if let Expr::Ident(ident) = &**callee {
                let callee_name = ident.sym.to_string();
                let args_len = call_expr.args.len();
                if args_len > 0 {
                    if callee_name == "cube" || callee_name == "view" {
                        if args_len != 2 {
                            self.emit_error(
                                call_expr.span,
                                &format!(
                                    "Incorrect number of arguments to {}() function",
                                    callee_name
                                ),
                            );
                            return;
                        }
                        let cube_name_opt: Option<String> = {
                            let first_arg = &call_expr.args[0].expr;
                            match &**first_arg {
                                Expr::Lit(Lit::Str(s)) => Some(s.value.to_string()),
                                Expr::Tpl(tpl) => {
                                    if !tpl.quasis.is_empty() {
                                        tpl.quasis[0].cooked.as_ref().map(|c| c.clone().to_string())
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            }
                        };
                        if let Some(last_arg) = call_expr.args.last_mut() {
                            {
                                let mut sql_visitor =
                                    self.sql_and_references_field_visitor(cube_name_opt.clone());
                                last_arg.visit_mut_with(&mut sql_visitor);
                            }
                            {
                                let mut known_visitor =
                                    self.known_identifiers_inject_visitor("extends".to_string());
                                last_arg.visit_mut_with(&mut known_visitor);
                            }
                        }
                    } else if callee_name == "context" {
                        if let Some(last_arg) = call_expr.args.last_mut() {
                            let mut sql_visitor = {
                                let self_ref = &mut *self;
                                self_ref.sql_and_references_field_visitor(None)
                            };

                            last_arg.expr.visit_mut_with(&mut sql_visitor);
                        }
                    }
                }
            }
        }
        call_expr.visit_mut_children_with(self)
    }
}

pub struct SqlAndReferencesFieldVisitor<'b, 'a: 'b> {
    pub cube_name: Option<String>,
    pub parent: &'b mut CubePropTransformVisitor<'a>,
    pub path_stack: Vec<String>,
}

impl SqlAndReferencesFieldVisitor<'_, '_> {
    fn current_path(&self) -> String {
        self.path_stack.join(".")
    }
}

impl VisitMut for SqlAndReferencesFieldVisitor<'_, '_> {
    fn visit_mut_prop(&mut self, prop: &mut Prop) {
        let mut added = false;
        if let Prop::KeyValue(ref kv) = prop {
            if let PropName::Ident(ref ident) = kv.key {
                let key_name = ident.sym.to_string();
                self.path_stack.push(key_name.clone());
                added = true;
                if TRANSPILLED_FIELDS.contains(&key_name) {
                    let full_path = self.current_path();
                    for pattern in TRANSPILLED_FIELDS_PATTERNS.iter() {
                        if pattern.is_match(&full_path) {
                            let parent_ptr = self.parent as *mut CubePropTransformVisitor;
                            let resolve = |n: &str| unsafe {
                                (*parent_ptr).resolve_symbol(
                                    self.cube_name.as_deref().unwrap_or(""),
                                    n,
                                    DUMMY_SP,
                                ) || (*parent_ptr).is_current_cube(n)
                            };
                            self.parent.transform_object_property(prop, &resolve);
                            self.path_stack.pop();
                            return;
                        }
                    }
                }
            }
        }
        prop.visit_mut_children_with(self);

        if !self.path_stack.is_empty() && added {
            self.path_stack.pop();
        }
    }

    fn visit_mut_array_lit(&mut self, arr: &mut ArrayLit) {
        for (idx, el) in arr.elems.iter_mut().enumerate() {
            if let Some(el) = el {
                self.path_stack.push(idx.to_string());
                el.visit_mut_children_with(self);
                self.path_stack.pop();
            }
        }
    }
}

pub struct KnownIdentifiersInjectVisitor<'b, 'a: 'b> {
    pub field: String,
    pub parent: &'b mut CubePropTransformVisitor<'a>,
}

impl VisitMut for KnownIdentifiersInjectVisitor<'_, '_> {
    fn visit_mut_prop(&mut self, prop: &mut Prop) {
        let ident_name = match &prop {
            Prop::Shorthand(ident) => ident.sym.clone().to_string(),
            Prop::KeyValue(key_value_prop) => match &key_value_prop.key {
                PropName::Ident(ident_name) => ident_name.sym.clone().to_string(),
                PropName::Str(str) => str.value.clone().to_string(),
                _ => "".to_string(),
            },
            _ => "".to_string(),
        };

        if ident_name.contains(&self.field) {
            let parent_ptr = self.parent as *mut CubePropTransformVisitor;
            let resolve = move |n: &str| unsafe { (*parent_ptr).resolve_cube(n) };
            self.parent.transform_object_property(prop, &resolve);
        }

        prop.visit_mut_children_with(self)
    }
}

pub struct CollectIdentifiersVisitor<'a> {
    pub identifiers: IndexSet<(Atom, SyntaxContext)>,
    pub resolve: &'a dyn Fn(&str) -> bool,
}

impl Visit for CollectIdentifiersVisitor<'_> {
    fn visit_ident(&mut self, ident: &Ident) {
        if (self.resolve)(&ident.sym) {
            self.identifiers.insert((ident.sym.clone(), ident.ctxt));
        }
    }

    fn visit_member_expr(&mut self, member: &MemberExpr) {
        member.obj.visit_with(self);
        match &member.prop {
            MemberProp::Ident(_ident_name) => member.prop.visit_with(self),
            MemberProp::PrivateName(_private_name) => {}
            MemberProp::Computed(_computed_prop_name) => member.prop.visit_with(self),
        };
    }
}
