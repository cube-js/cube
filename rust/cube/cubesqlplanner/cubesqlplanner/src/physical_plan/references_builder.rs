use crate::physical_plan::symbols::column_ref_symbol::column_reference;
use crate::physical_plan::{
    CalcGroupsJoin, From, FromSource, Join, QualifiedColumnName, SingleAliasedSource, SingleSource,
};
use crate::planner::filter::{Filter, FilterItem};
use crate::planner::MemberSymbol;
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

/// What one select replaces its members with: `full_name` of a member
/// the select reads from one of its sources → the reference symbol that
/// reads it. Applied to the select's whole symbol environment at once,
/// so a member named here is read wherever the select mentions it.
pub type ReferenceSubstitutions = HashMap<String, Rc<MemberSymbol>>;

pub struct ReferencesBuilder {
    source: Rc<From>,
}

impl ReferencesBuilder {
    pub fn new(source: Rc<From>) -> Self {
        Self { source }
    }

    pub fn validate_member(
        &self,
        member: Rc<MemberSymbol>,
        strict_source: &Option<String>,
    ) -> Result<(), CubeError> {
        if self
            .find_reference_for_member(&member, strict_source)
            .is_some()
        {
            return Ok(());
        }

        let dependencies = member.get_dependencies();
        if !dependencies.is_empty() {
            for dep in dependencies.iter() {
                self.validate_member(dep.clone(), strict_source)?;
            }
        } else {
            if !self.has_source_for_leaf_memeber(&member, strict_source) {
                /*                 return Err(CubeError::internal(format!(
                    "Planning error: member {} has no source",
                    member_name
                ))); */
            }
        }
        Ok(())
    }

    /// Records how `member` is read from this select's sources: the
    /// member itself when a source produces it, otherwise the
    /// dependencies of its expression that a source produces.
    ///
    /// The first entry for a name wins, so a caller that pins a member
    /// itself (a value fixed by the query, an axis column of a join)
    /// records it before the walk reaches it.
    pub fn collect_substitutions_for_member(
        &self,
        member: Rc<MemberSymbol>,
        strict_source: &Option<String>,
        substitutions: &mut ReferenceSubstitutions,
    ) -> Result<(), CubeError> {
        // A reference already names the column it reads. Resolving it
        // again would look its origin up in this select's sources and
        // point it somewhere else.
        if matches!(member.as_ref(), MemberSymbol::ColumnRef(_)) {
            return Ok(());
        }
        let member_name = member.full_name();
        if substitutions.contains_key(&member_name) {
            return Ok(());
        }
        if let Some(column) = self.find_reference_for_member(&member, strict_source) {
            substitutions.insert(member_name, column_reference(&member, column));
            return Ok(());
        }

        for dep in member.get_dependencies().iter() {
            self.collect_substitutions_for_member(dep.clone(), strict_source, substitutions)?;
        }
        Ok(())
    }

    pub fn collect_substitutions_for_filter(
        &self,
        filter: &Option<Filter>,
        substitutions: &mut ReferenceSubstitutions,
    ) -> Result<(), CubeError> {
        if let Some(filter) = filter {
            for itm in filter.items.iter() {
                self.collect_substitutions_for_filter_item(itm, substitutions)?;
            }
        }
        Ok(())
    }

    fn collect_substitutions_for_filter_item(
        &self,
        item: &FilterItem,
        substitutions: &mut ReferenceSubstitutions,
    ) -> Result<(), CubeError> {
        match item {
            FilterItem::Item(item) => self.collect_substitutions_for_member(
                item.member_evaluator().clone(),
                &None,
                substitutions,
            )?,
            FilterItem::Group(group) => {
                for itm in group.items.iter() {
                    self.collect_substitutions_for_filter_item(itm, substitutions)?
                }
            }
            FilterItem::Segment(segment) => self.collect_substitutions_for_member(
                segment.member_evaluator().clone(),
                &None,
                substitutions,
            )?,
        }
        Ok(())
    }

    pub fn validete_member_for_leaf_query(
        &self,
        member: Rc<MemberSymbol>,
        strict_source: &Option<String>,
    ) -> Result<(), CubeError> {
        let dependencies = member.get_dependencies();
        if !dependencies.is_empty() {
            for dep in dependencies.iter() {
                self.validete_member_for_leaf_query(dep.clone(), strict_source)?
            }
        } else {
            /*             if !self.has_source_for_leaf_memeber(&member, strict_source) {
                return Err(CubeError::internal(format!(
                    "Planning error: member {} has no source",
                    member.full_name()
                )));
            } */
        }
        Ok(())
    }

    pub fn validate_filter(&self, filter: &Filter) -> Result<(), CubeError> {
        for itm in filter.items.iter() {
            self.validate_filter_item(itm)?;
        }
        Ok(())
    }

    fn validate_filter_item(&self, item: &FilterItem) -> Result<(), CubeError> {
        match item {
            FilterItem::Item(item) => {
                self.validate_member(item.member_evaluator().clone(), &None)?
            }
            FilterItem::Group(group) => {
                for itm in group.items.iter() {
                    self.validate_filter_item(itm)?
                }
            }
            FilterItem::Segment(segment) => {
                self.validate_member(segment.member_evaluator().clone(), &None)?
            }
        }
        Ok(())
    }

    fn has_source_for_leaf_memeber(
        &self,
        member: &Rc<MemberSymbol>,
        strict_source: &Option<String>,
    ) -> bool {
        self.has_source_for_leaf_memeber_in_from(&self.source, member, strict_source)
    }

    fn has_source_for_leaf_memeber_in_from(
        &self,
        from: &Rc<From>,
        member: &Rc<MemberSymbol>,
        strict_source: &Option<String>,
    ) -> bool {
        match &from.source {
            FromSource::Empty => false,
            FromSource::Single(source) => {
                self.is_single_source_has_leaf_member(&source, member, strict_source)
            }
            FromSource::Join(join) => {
                self.is_single_source_has_leaf_member(&join.root, member, strict_source)
                    || join.joins.iter().any(|itm| {
                        self.is_single_source_has_leaf_member(&itm.from, member, strict_source)
                    })
            }
            FromSource::CalcGroupsJoin(calc_groups) => {
                self.has_source_for_leaf_memeber_in_from(&calc_groups.from(), member, strict_source)
            }
        }
    }

    fn is_single_source_has_leaf_member(
        &self,
        source: &SingleAliasedSource,
        member: &Rc<MemberSymbol>,
        strict_source: &Option<String>,
    ) -> bool {
        if let Some(strict_source) = strict_source {
            if strict_source != &source.alias {
                return false;
            }
        }

        match &source.source {
            SingleSource::Cube(cube) => {
                cube.name() == &member.cube_name() && cube.has_member(&member.name())
            }
            _ => false,
        }
    }

    pub fn resolve_alias_for_member(
        &self,
        member: &Rc<MemberSymbol>,
        strict_source: &Option<String>,
    ) -> Option<String> {
        if let Some(reference) = self.find_reference_for_member(member, strict_source) {
            Some(reference.name().clone())
        } else {
            None
        }
    }

    pub fn find_reference_for_member(
        &self,
        member: &Rc<MemberSymbol>,
        strict_source: &Option<String>,
    ) -> Option<QualifiedColumnName> {
        self.find_reference_column_for_member_in_from(&self.source, member, strict_source)
    }

    fn find_reference_column_for_member_in_from(
        &self,
        from: &Rc<From>,
        member: &Rc<MemberSymbol>,
        strict_source: &Option<String>,
    ) -> Option<QualifiedColumnName> {
        match &from.source {
            FromSource::Empty => None,
            FromSource::Single(source) => self.find_reference_column_for_member_in_single_source(
                &source,
                member,
                strict_source,
            ),
            FromSource::Join(join) => {
                self.find_reference_column_for_member_in_join(&join, member, strict_source)
            }
            FromSource::CalcGroupsJoin(calc_groups) => self
                .find_reference_column_for_member_in_calc_groups(
                    calc_groups,
                    member,
                    strict_source,
                ),
        }
    }

    fn find_reference_column_for_member_in_single_source(
        &self,
        source: &SingleAliasedSource,
        member: &Rc<MemberSymbol>,
        strict_source: &Option<String>,
    ) -> Option<QualifiedColumnName> {
        if let Some(strict_source) = strict_source {
            if strict_source != &source.alias {
                return None;
            }
        }
        let column_name = match &source.source {
            SingleSource::Subquery(query_plan) => {
                query_plan.schema().resolve_member_reference(member)
            }
            SingleSource::Cube(_) => None,
            SingleSource::TableReference(_, schema) => schema.resolve_member_reference(member),
            // Opaque pre-rendered SQL: members are referenced via the alias as
            // literals in the ON/projection SQL, not resolved against a schema.
            SingleSource::RawSubquerySql(_) => None,
        };
        column_name.map(|col| QualifiedColumnName::new(Some(source.alias.clone()), col))
    }

    fn find_reference_column_for_member_in_join(
        &self,
        join: &Rc<Join>,
        member: &Rc<MemberSymbol>,
        strict_source: &Option<String>,
    ) -> Option<QualifiedColumnName> {
        if let Some(root_ref) = self.find_reference_column_for_member_in_single_source(
            &join.root,
            member,
            strict_source,
        ) {
            return Some(root_ref);
        }
        join.joins.iter().find_map(|item| {
            self.find_reference_column_for_member_in_single_source(
                &item.from,
                member,
                strict_source,
            )
        })
    }

    fn find_reference_column_for_member_in_calc_groups(
        &self,
        calc_groups: &Rc<CalcGroupsJoin>,
        member: &Rc<MemberSymbol>,
        strict_source: &Option<String>,
    ) -> Option<QualifiedColumnName> {
        if strict_source.is_none() {
            if let Some(group_itm) = calc_groups.calc_groups().iter().find_map(|itm| {
                if &itm.symbol == member {
                    Some(QualifiedColumnName::new(
                        Some(itm.group_alias()),
                        itm.symbol.name(),
                    ))
                } else {
                    None
                }
            }) {
                return Some(group_itm);
            }
        }
        self.find_reference_column_for_member_in_from(&calc_groups.from(), member, strict_source)
    }
}
