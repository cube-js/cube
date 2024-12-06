// Open spec generator generates ToString methods for enums, let's disable clippy rule as quick
// workaround. TODO: Use new one open spec generator?
#![allow(clippy::to_string_trait_impl)]

#[macro_use]
extern crate serde_derive;

extern crate reqwest;
extern crate serde;
extern crate serde_json;
extern crate url;

pub mod apis;
pub mod models;
