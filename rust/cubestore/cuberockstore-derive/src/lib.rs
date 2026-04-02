use proc_macro::TokenStream;

mod secondary_index_key;

#[proc_macro_derive(SecondaryIndexKey)]
pub fn derive_secondary_index_key(input: TokenStream) -> TokenStream {
    secondary_index_key::derive_secondary_index_key(input)
}