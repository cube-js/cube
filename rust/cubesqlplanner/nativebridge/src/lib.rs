use inflector::Inflector;
use itertools::Itertools;
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{format_ident, quote, ToTokens};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{parse_macro_input, FnArg, Item, Pat, ReturnType, TraitItem};
#[proc_macro_attribute]
pub fn native_bridge(_attr: TokenStream, input: TokenStream) -> proc_macro::TokenStream {
    let svc = parse_macro_input!(input as NativeService);

    proc_macro::TokenStream::from(svc.into_token_stream())
}

struct NativeService {
    ident: Ident,
    methods: Vec<NativeMethod>,
}

struct NativeMethod {
    ident: Ident,
    asyncness: bool,
    args: Vec<FnArg>,
    output: ReturnType,
}

impl Parse for NativeService {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let trait_item = input.call(Item::parse)?;
        let svc = match trait_item {
            Item::Trait(trait_item) => {
                let methods = trait_item
                    .items
                    .iter()
                    .filter_map(|item| match item {
                        TraitItem::Method(method_item) => Some(NativeMethod {
                            ident: method_item.sig.ident.clone(),
                            args: method_item.sig.inputs.iter().cloned().collect::<Vec<_>>(),
                            output: method_item.sig.output.clone(),
                            asyncness: method_item.sig.asyncness.is_some(),
                        }),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                NativeService {
                    ident: trait_item.ident.clone(),
                    methods,
                }
            }
            x => {
                return Err(syn::Error::new(
                    x.span(),
                    "Only trait can be annotated as a service",
                ))
            }
        };
        Ok(svc)
    }
}

impl NativeService {
    fn original_trait(&self) -> proc_macro2::TokenStream {
        let service_ident = &self.ident;
        let methods = self
            .methods
            .iter()
            .map(|m| m.original_method())
            .collect::<Vec<_>>();
        // TODO Supertraits
        quote! {
            #[async_trait]
            pub trait #service_ident {
                #( #methods )*
            }
        }
    }

    fn struct_body(&self) -> proc_macro2::TokenStream {
        let struct_ident = self.struct_ident();
        quote! {
            pub struct #struct_ident {
                native_object: NativeObjectHandler,
            }
        }
    }

    fn native_holder_impl(&self) -> proc_macro2::TokenStream {
        let struct_ident = self.struct_ident();
        quote! {
            impl NativeObjectHolder for #struct_ident {

                fn new_from_native(native_object: NativeObjectHandler) -> Self {
                    Self {native_object}

                }

                fn get_native_object(&self) -> &NativeObjectHandler {
                    &self.native_object
                }
            }
        }
    }

    fn struct_ident(&self) -> Ident {
        format_ident!("Native{}", &self.ident)
    }

    fn struct_impl(&self) -> proc_macro2::TokenStream {
        let service_ident = &self.ident;
        let struct_ident = self.struct_ident();
        let methods = self
            .methods
            .iter()
            .map(|m| m.method_impl())
            .collect::<Vec<_>>();
        quote! {


            #[async_trait]
            impl #service_ident for #struct_ident {
                #( #methods )*
            }
        }
    }
}

impl NativeMethod {
    fn original_method(&self) -> proc_macro2::TokenStream {
        let &Self {
            ident,
            asyncness,
            args,
            output,
        } = &self;
        if *asyncness {
            quote! {
                async fn #ident(#( #args ),*) #output;
            }
        } else {
            quote! {
                fn #ident(#( #args ),*) #output;
            }
        }
    }

    fn method_impl(&self) -> proc_macro2::TokenStream {
        let &Self {
            ident,
            asyncness,
            args,
            output,
        } = &self;
        let typed_args = args
            .iter()
            .filter_map(|a| match a {
                FnArg::Typed(ty) => match ty.pat.as_ref() {
                    Pat::Ident(id) => Some(id.ident.clone()),
                    x => panic!("Unexpected pattern: {:?}", x),
                },
                FnArg::Receiver(_) => None,
            })
            .collect::<Vec<_>>();
        let js_args_set = typed_args
            .iter()
            .map(|a| Self::js_agr_set(a))
            .collect::<Vec<_>>();
        let js_args_names = typed_args
            .iter()
            .map(|a| Self::native_arg_ident(a))
            .collect::<Vec<_>>();
        let js_method_name = self.camel_case_name();

        if !*asyncness {
            quote! {
                fn #ident(#( #args ),*) #output {
                    let context_holder = self.native_object.get_context()?;
                    let args = vec![#( #js_args_set ),*];

                    let res = self.native_object.to_struct()?
                        .call_method(
                            #js_method_name,
                            args
                        )?;
                   let deserializer = NativeDeserializer::new(res);
                   deserializer.deserialize().map_err(|e| {
                       CubeError::internal(format!("Error deserializing result: {}", e))
                   })
                }
            }
        } else {
            quote! {
                fn #ident(#( #args ),*) #output {
                    unimplemented!()
                }
            }
        }
    }

    fn js_agr_set(arg: &Ident) -> proc_macro2::TokenStream {
        let native_arg = Self::native_arg_ident(arg);
        quote! {
            NativeSerializer::serialize(&#arg, context_holder.clone()).map_err(|e| CubeError::internal(format!("Error serializing argument: {}", e)))?
        }
    }

    fn native_arg_ident(arg: &Ident) -> Ident {
        format_ident!("native_{}", arg)
    }

    fn camel_case_name(&self) -> String {
        let name = self.ident.to_string();
        let worlds = name.split('_');
        let res = worlds
            .clone()
            .take(1)
            .map(|s| s.to_string())
            .chain(worlds.skip(1).map(|s| Self::uppercase(s)))
            .join("");
        res
    }

    fn uppercase(name: &str) -> String {
        let mut c = name.chars();
        match c.next() {
            None => String::new(),
            Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        }
    }

    fn variant_ident(&self) -> Ident {
        format_ident!("{}", self.ident.to_string().to_camel_case())
    }
}

impl ToTokens for NativeService {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        tokens.extend(vec![
            self.original_trait(),
            self.struct_body(),
            self.native_holder_impl(),
            self.struct_impl(),
            /* self.method_call_enum(),
            self.method_result_enum(),
            self.client_transport_trait(),
            self.client_impl(),
            self.server_impl(), */
        ]);
    }
}
