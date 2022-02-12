use inflector::Inflector;
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{format_ident, quote, ToTokens};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::{parse_macro_input, FnArg, Item, Pat, ReturnType, TraitItem};

#[proc_macro_attribute]
pub fn service(_attr: TokenStream, input: TokenStream) -> proc_macro::TokenStream {
    let svc = parse_macro_input!(input as RpcService);

    proc_macro::TokenStream::from(svc.into_token_stream())
}

struct RpcService {
    ident: Ident,
    methods: Vec<RpcMethod>,
}

struct RpcMethod {
    ident: Ident,
    asyncness: bool,
    args: Vec<FnArg>,
    output: ReturnType,
}

impl Parse for RpcService {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let trait_item = input.call(Item::parse)?;
        let svc = match trait_item {
            Item::Trait(trait_item) => {
                let methods = trait_item
                    .items
                    .iter()
                    .filter_map(|item| match item {
                        TraitItem::Method(method_item) => Some(RpcMethod {
                            ident: method_item.sig.ident.clone(),
                            args: method_item.sig.inputs.iter().cloned().collect::<Vec<_>>(),
                            output: method_item.sig.output.clone(),
                            asyncness: method_item.sig.asyncness.is_some(),
                        }),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                RpcService {
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

impl RpcService {
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
            pub trait #service_ident: DIService + Send + Sync {
                #( #methods )*
            }
        }
    }

    fn method_call_enum(&self) -> proc_macro2::TokenStream {
        let method_call = self.method_call_ident();
        let methods = self
            .methods
            .iter()
            .map(|m| m.method_call_enum())
            .collect::<Vec<_>>();
        quote! {
            #[derive(Serialize, Deserialize, Debug)]
            pub enum #method_call {
                #( #methods ),*
            }
        }
    }

    fn method_result_enum(&self) -> proc_macro2::TokenStream {
        let method_call = self.method_result_ident();
        let methods = self
            .methods
            .iter()
            .map(|m| m.method_result_enum())
            .collect::<Vec<_>>();
        quote! {
            #[derive(Serialize, Deserialize, Debug)]
            pub enum #method_call {
                #( #methods ),*
            }
        }
    }

    fn client_transport_trait(&self) -> proc_macro2::TokenStream {
        let method_call = self.method_call_ident();
        let method_result = self.method_result_ident();
        let client_transport = self.client_transport_ident();
        quote! {
            #[async_trait]
            pub trait #client_transport: Send + Sync {
                async fn invoke_method(&self, method_call: #method_call) -> Result<#method_result, CubeError>;
            }
        }
    }

    fn client_impl(&self) -> proc_macro2::TokenStream {
        let service_ident = &self.ident;
        let client_impl = format_ident!("{}RpcClient", self.ident);
        let client_transport = self.client_transport_ident();
        let methods = self
            .methods
            .iter()
            .map(|m| m.client_impl(self.method_call_ident(), self.method_result_ident()))
            .collect::<Vec<_>>();
        quote! {
            pub struct #client_impl {
                transport: Arc<dyn #client_transport>
            }

            impl #client_impl {
                pub fn new(transport: Arc<dyn #client_transport>) -> Self {
                    Self { transport }
                }
            }

            #[async_trait]
            impl #service_ident for #client_impl {
                #( #methods )*
            }
        }
    }

    fn server_impl(&self) -> proc_macro2::TokenStream {
        let service_ident = &self.ident;
        let server_impl = format_ident!("{}RpcServer", self.ident);
        let method_call = self.method_call_ident();
        let method_result = self.method_result_ident();
        let methods = self
            .methods
            .iter()
            .map(|m| m.server_impl(self.method_call_ident(), self.method_result_ident()))
            .collect::<Vec<_>>();
        quote! {
            pub struct #server_impl {
                service: Arc<dyn #service_ident>
            }

            impl #server_impl {
                pub fn new(service: Arc<dyn #service_ident>) -> Self {
                    Self { service }
                }

                pub async fn invoke_method(&self, method_call: #method_call) -> #method_result {
                    match method_call {
                        #( #methods ),*
                    }
                }
            }
        }
    }

    fn method_call_ident(&self) -> Ident {
        format_ident!("{}RpcMethodCall", self.ident)
    }

    fn method_result_ident(&self) -> Ident {
        format_ident!("{}RpcMethodResult", self.ident)
    }

    fn client_transport_ident(&self) -> Ident {
        format_ident!("{}RpcClientTransport", self.ident)
    }
}

impl RpcMethod {
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

    fn method_call_enum(&self) -> proc_macro2::TokenStream {
        let &Self { ident, args, .. } = &self;

        let arg_types = args
            .iter()
            .filter_map(|a| match a {
                FnArg::Typed(ty) => Some(ty.ty.clone()),
                FnArg::Receiver(_) => None,
            })
            .collect::<Vec<_>>();

        let var_ident = format_ident!("{}", ident.to_string().to_camel_case());

        if arg_types.is_empty() {
            quote! {
                #var_ident
            }
        } else {
            quote! {
                #var_ident(#( #arg_types ),*)
            }
        }
    }

    fn method_result_enum(&self) -> proc_macro2::TokenStream {
        let result_type = match &self.output {
            ReturnType::Type(_, ty) => ty.clone(),
            ReturnType::Default => panic!("Default return type is not supported"),
        };

        let var_ident = self.variant_ident();

        if self.asyncness {
            quote! {
                #var_ident(#result_type)
            }
        } else {
            // TODO
            quote! {
                #var_ident
            }
        }
    }

    fn client_impl(
        &self,
        method_call_ident: Ident,
        method_result_ident: Ident,
    ) -> proc_macro2::TokenStream {
        let &Self {
            ident,
            asyncness,
            args,
            output,
        } = &self;
        let arg_names = args
            .iter()
            .filter_map(|a| match a {
                FnArg::Typed(ty) => match ty.pat.as_ref() {
                    Pat::Ident(id) => Some(id.ident.clone()),
                    x => panic!("Unexpected pattern: {:?}", x),
                },
                FnArg::Receiver(_) => None,
            })
            .collect::<Vec<_>>();
        let variant = self.variant_ident();

        let new_method_call = if arg_names.is_empty() {
            quote! {
                #method_call_ident::#variant
            }
        } else {
            quote! {
                #method_call_ident::#variant(#( #arg_names ),*)
            }
        };

        if *asyncness {
            quote! {
                async fn #ident(#( #args ),*) #output {
                    let method_call = #new_method_call;
                    let result = self.transport.invoke_method(method_call).await?;
                    match result {
                        #method_result_ident::#variant(res) => res,
                        x => panic!("Unexpected result: {:?}", x)
                    }
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

    fn server_impl(
        &self,
        method_call_ident: Ident,
        method_result_ident: Ident,
    ) -> proc_macro2::TokenStream {
        let &Self {
            ident,
            asyncness,
            args,
            ..
        } = &self;
        let arg_names = args
            .iter()
            .filter_map(|a| match a {
                FnArg::Typed(ty) => match ty.pat.as_ref() {
                    Pat::Ident(id) => Some(id.ident.clone()),
                    x => panic!("Unexpected pattern: {:?}", x),
                },
                FnArg::Receiver(_) => None,
            })
            .collect::<Vec<_>>();
        let variant = self.variant_ident();

        let match_method = if arg_names.is_empty() {
            quote! {
                #method_call_ident::#variant
            }
        } else {
            quote! {
                #method_call_ident::#variant(#( #arg_names ),*)
            }
        };

        if *asyncness {
            quote! {
                #match_method => #method_result_ident::#variant(self.service.#ident(#( #arg_names ),*).await)
            }
        } else {
            quote! {
                #match_method => unimplemented!()
            }
        }
    }

    fn variant_ident(&self) -> Ident {
        format_ident!("{}", self.ident.to_string().to_camel_case())
    }
}

impl ToTokens for RpcService {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        tokens.extend(vec![
            self.original_trait(),
            self.method_call_enum(),
            self.method_result_enum(),
            self.client_transport_trait(),
            self.client_impl(),
            self.server_impl(),
        ]);
    }
}
