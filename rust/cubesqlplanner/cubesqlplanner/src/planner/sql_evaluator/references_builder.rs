use crate::plan::{
    Filter, FilterItem, From, Join, QualifiedColumnName, SingleAliasedSource, SingleSource,
};
use cubenativeutils::CubeError;
use std::collections::HashMap;
use std::rc::Rc;

use super::MemberSymbol;

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
        let member_name = member.full_name();
        if self
            .find_reference_for_member(&member_name, strict_source)
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

    pub fn resolve_references_for_member(
        &self,
        member: Rc<MemberSymbol>,
        strict_source: &Option<String>,
        references: &mut HashMap<String, QualifiedColumnName>,
    ) -> Result<(), CubeError> {
        let member_name = member.full_name();
        if references.contains_key(&member_name) {
            return Ok(());
        }
        if let Some(reference) = self.find_reference_for_member(&member_name, strict_source) {
            references.insert(member_name.clone(), reference);
            return Ok(());
        }

        let dependencies = member.get_dependencies();
        if !dependencies.is_empty() {
            for dep in dependencies.iter() {
                self.resolve_references_for_member(dep.clone(), strict_source, references)?
            }
        } else {
            /*             if !self.has_source_for_leaf_memeber(&member, strict_source) {
                return Err(CubeError::internal(format!(
                    "Planning error: member {} has no source",
                    member_name
                )));
            } */
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

    pub fn resolve_references_for_filter(
        &self,
        filter: &Option<Filter>,
        references: &mut HashMap<String, QualifiedColumnName>,
    ) -> Result<(), CubeError> {
        if let Some(filter) = filter {
            for itm in filter.items.iter() {
                self.resolve_references_for_filter_item(itm, references)?;
            }
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

    fn resolve_references_for_filter_item(
        &self,
        item: &FilterItem,
        references: &mut HashMap<String, QualifiedColumnName>,
    ) -> Result<(), CubeError> {
        match item {
            FilterItem::Item(item) => self.resolve_references_for_member(
                item.member_evaluator().clone(),
                &None,
                references,
            )?,
            FilterItem::Group(group) => {
                for itm in group.items.iter() {
                    self.resolve_references_for_filter_item(itm, references)?
                }
            }
            FilterItem::Segment(segment) => self.resolve_references_for_member(
                segment.member_evaluator().clone(),
                &None,
                references,
            )?,
        }
        Ok(())
    }

    fn has_source_for_leaf_memeber(
        &self,
        member: &Rc<MemberSymbol>,
        strict_source: &Option<String>,
    ) -> bool {
        match &self.source.source {
            crate::plan::FromSource::Empty => false,
            crate::plan::FromSource::Single(source) => {
                self.is_single_source_has_leaf_member(&source, member, strict_source)
            }
            crate::plan::FromSource::Join(join) => {
                self.is_single_source_has_leaf_member(&join.root, member, strict_source)
                    || join.joins.iter().any(|itm| {
                        self.is_single_source_has_leaf_member(&itm.from, member, strict_source)
                    })
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
        member_name: &String,
        strict_source: &Option<String>,
    ) -> Option<String> {
        if let Some(reference) = self.find_reference_for_member(member_name, strict_source) {
            Some(reference.name().clone())
        } else {
            None
        }
    }

    pub fn find_reference_for_member(
        &self,
        member_name: &String,
        strict_source: &Option<String>,
    ) -> Option<QualifiedColumnName> {
        match &self.source.source {
            crate::plan::FromSource::Empty => None,
            crate::plan::FromSource::Single(source) => self
                .find_reference_column_for_member_in_single_source(
                    &source,
                    member_name,
                    strict_source,
                ),
            crate::plan::FromSource::Join(join) => {
                self.find_reference_column_for_member_in_join(&join, member_name, strict_source)
            }
        }
    }

    fn find_reference_column_for_member_in_single_source(
        &self,
        source: &SingleAliasedSource,
        member_name: &String,
        strict_source: &Option<String>,
    ) -> Option<QualifiedColumnName> {
        if let Some(strict_source) = strict_source {
            if strict_source != &source.alias {
                return None;
            }
        }
        let column_name = match &source.source {
            SingleSource::Subquery(query_plan) => {
                query_plan.schema().resolve_member_reference(member_name)
            }
            SingleSource::Cube(_) => None,
            SingleSource::TableReference(_, schema) => schema.resolve_member_reference(member_name),
        };
        column_name.map(|col| QualifiedColumnName::new(Some(source.alias.clone()), col))
    }

    fn find_reference_column_for_member_in_join(
        &self,
        join: &Rc<Join>,
        member_name: &String,
        strict_source: &Option<String>,
    ) -> Option<QualifiedColumnName> {
        if let Some(root_ref) = self.find_reference_column_for_member_in_single_source(
            &join.root,
            member_name,
            strict_source,
        ) {
            return Some(root_ref);
        }
        join.joins.iter().find_map(|item| {
            self.find_reference_column_for_member_in_single_source(
                &item.from,
                member_name,
                strict_source,
            )
        })
    }
}
