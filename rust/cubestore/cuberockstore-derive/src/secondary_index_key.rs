use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

fn has_nullable_attr(variant: &syn::Variant) -> bool {
    variant
        .attrs
        .iter()
        .any(|attr| attr.path().is_ident("nullable"))
}

/// Derive macro that generates `to_bytes()` and `is_nullable()` for index key enums.
///
/// Each unnamed field in every variant must implement `IndexKeyToBytes`.
/// Fields are serialized sequentially into a `Vec<u8>`.
///
/// Use `#[nullable]` on a variant to mark it as nullable.
///
/// # Example
/// ```ignore
/// #[derive(SecondaryIndexKey)]
/// pub enum MyIndexKey {
///     ByName(String),
///     ByIdAndName(u64, String),
///     #[nullable]
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

    let to_bytes_arms = data.variants.iter().map(|variant| {
        let variant_ident = &variant.ident;

        match &variant.fields {
            Fields::Unnamed(fields) => {
                let field_bindings: Vec<_> = (0..fields.unnamed.len())
                    .map(|i| syn::Ident::new(&format!("f{}", i), variant_ident.span()))
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

    let has_any_nullable = data.variants.iter().any(has_nullable_attr);

    let is_nullable_method = if has_any_nullable {
        let nullable_arms = data.variants.iter().map(|variant| {
            let variant_ident = &variant.ident;
            let is_nullable = has_nullable_attr(variant);

            let pattern = match &variant.fields {
                Fields::Unnamed(_) => quote! { #name::#variant_ident(..) },
                Fields::Unit => quote! { #name::#variant_ident },
                Fields::Named(_) => quote! { #name::#variant_ident { .. } },
            };

            quote! { #pattern => #is_nullable }
        });

        quote! {
            pub fn is_nullable(&self) -> bool {
                match self {
                    #(#nullable_arms),*
                }
            }
        }
    } else {
        quote! {
            pub fn is_nullable(&self) -> bool {
                false
            }
        }
    };

    let expanded = quote! {
        impl #name {
            pub fn to_bytes(&self) -> Vec<u8> {
                match self {
                    #(#to_bytes_arms),*
                }
            }

            #is_nullable_method
        }
    };

    expanded.into()
}
