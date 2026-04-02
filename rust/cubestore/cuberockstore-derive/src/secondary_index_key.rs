use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

/// Derive macro that generates `to_bytes()` for index key enums.
///
/// Each unnamed field in every variant must implement `IndexKeyToBytes`.
/// Fields are serialized sequentially into a `Vec<u8>`.
///
/// # Example
/// ```ignore
/// #[derive(SecondaryIndexKey)]
/// pub enum MyIndexKey {
///     ByName(String),
///     ByIdAndName(u64, String),
///     ByOptionalId(Option<u64>),
/// }
/// ```
pub fn derive_secondary_index_key(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    let data = match &input.data {
        Data::Enum(data) => data,
        _ => {
            return syn::Error::new_spanned(
                &input,
                "SecondaryIndexKey can only be derived for enums",
            )
            .to_compile_error()
            .into();
        }
    };

    let match_arms = data.variants.iter().map(|variant| {
        let variant_ident = &variant.ident;

        match &variant.fields {
            Fields::Unnamed(fields) => {
                let field_bindings: Vec<_> = (0..fields.unnamed.len())
                    .map(|i| {
                        let ident = syn::Ident::new(&format!("f{}", i), variant_ident.span());
                        ident
                    })
                    .collect();

                let write_calls = field_bindings.iter().map(|binding| {
                    quote! {
                        cuberockstore::IndexKeyToBytes::write_index_key_bytes(#binding, &mut buf);
                    }
                });

                quote! {
                    #name::#variant_ident(#(#field_bindings),*) => {
                        let mut buf = Vec::new();
                        #(#write_calls)*
                        buf
                    }
                }
            }
            Fields::Unit => {
                quote! {
                    #name::#variant_ident => {
                        Vec::new()
                    }
                }
            }
            Fields::Named(_) => syn::Error::new_spanned(
                variant,
                "SecondaryIndexKey does not support named fields in variants",
            )
            .to_compile_error(),
        }
    });

    let expanded = quote! {
        impl #name {
            pub fn to_bytes(&self) -> Vec<u8> {
                match self {
                    #(#match_arms),*
                }
            }
        }
    };

    expanded.into()
}
