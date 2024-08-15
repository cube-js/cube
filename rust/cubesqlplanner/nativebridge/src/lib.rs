use itertools::Itertools;
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{format_ident, quote, ToTokens};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::token::Colon2;
use syn::{
    parse_macro_input, punctuated::Punctuated, FnArg, Item, Meta, Pat, Path, PathArguments,
    PathSegment, ReturnType, TraitItem, TraitItemMethod, Type,
};
#[proc_macro_attribute]
pub fn native_bridge(args: TokenStream, input: TokenStream) -> proc_macro::TokenStream {
    let mut svc = parse_macro_input!(input as NativeService);
    let args = parse_macro_input!(args with Punctuated::<Meta, syn::Token![,]>::parse_terminated);
    if args.len() > 0 {
        let arg = args.first().unwrap();
        match arg {
            Meta::Path(p) => svc.static_data_type = Some(p.clone()),
            _ => {}
        }
    }

    proc_macro::TokenStream::from(svc.into_token_stream())
}

struct NativeService {
    ident: Ident,
    methods: Vec<NativeMethod>,
    pub static_data_type: Option<Path>,
}

enum NativeMethodType {
    Call,
    Getter,
}

struct NativeMethodParams {
    pub method_type: NativeMethodType,
    pub is_optional: bool,
}

impl Default for NativeMethodParams {
    fn default() -> Self {
        Self {
            method_type: NativeMethodType::Call,
            is_optional: false,
        }
    }
}

struct NativeOutputParams {
    type_path: Path,
    dynamic_container_path: Option<Path>,
}

struct NativeMethod {
    ident: Ident,
    args: Vec<FnArg>,
    output: ReturnType,
    output_params: NativeOutputParams,
    method_params: NativeMethodParams,
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
                        TraitItem::Method(method_item) => {
                            let method_params = Self::parse_method_params(method_item);
                            Some(NativeMethod {
                                ident: method_item.sig.ident.clone(),
                                args: method_item.sig.inputs.iter().cloned().collect::<Vec<_>>(),
                                output: method_item.sig.output.clone(),
                                output_params: Self::get_output_for_deserializer(
                                    &method_item.sig.output,
                                    method_params.is_optional,
                                )
                                .unwrap(),
                                method_params,
                            })
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                NativeService {
                    ident: trait_item.ident.clone(),
                    methods,
                    static_data_type: None,
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
    fn parse_method_params(method_item: &TraitItemMethod) -> NativeMethodParams {
        let mut method_params = NativeMethodParams::default();

        if method_item.attrs.len() > 0 {
            let attr = method_item.attrs.first().unwrap();
            for attr in method_item.attrs.iter() {
                let seg = attr.path.segments.last().unwrap();
                match seg.ident.to_string().as_str() {
                    "optional" => {
                        method_params.is_optional = true;
                    }
                    "field" => {
                        method_params.method_type = NativeMethodType::Getter;
                    }
                    _ => {}
                }
            }
        }
        method_params
    }
    fn get_output_for_deserializer(
        tp: &ReturnType,
        optional: bool,
    ) -> syn::Result<NativeOutputParams> {
        let expected_type = if optional {
            "Result<_>"
        } else {
            "Result<Option<_>>"
        };
        let s = match tp {
            ReturnType::Default => Err(syn::Error::new(
                tp.span(),
                format!("Return type should be {}", expected_type),
            )),
            ReturnType::Type(_, tt) => match tt.as_ref() {
                syn::Type::Path(tp) => {
                    let segs = &tp.path.segments;
                    Self::get_deserializer_output_for_result(segs, optional)
                    /* if optional {
                        Self::get_deserializer_output_for_optional_call(segs)
                    } else {
                        Self::get_deserializer_output_for_regular_call(segs)
                    } */
                }
                _ => Err(syn::Error::new(
                    tp.span(),
                    format!("Return type should be {}", expected_type),
                )),
            },
        };
        s
    }

    fn get_deserializer_output_for_result(
        segs: &Punctuated<PathSegment, Colon2>,
        optional: bool,
    ) -> syn::Result<NativeOutputParams> {
        let seg = segs.last().ok_or(syn::Error::new(
            segs.span(),
            "Return type should be Result<_>",
        ))?;
        if seg.ident.to_string() == "Result" {
            let args = &seg.arguments;
            if optional {
                Self::get_deserializer_output_for_option(args)
            } else {
                Self::get_type_for_deserialize_from_result_args(args)
            }
        } else {
            Err(syn::Error::new(
                seg.span(),
                "Return type should be Result<_>",
            ))
        }
    }

    fn get_deserializer_output_for_option(args: &PathArguments) -> syn::Result<NativeOutputParams> {
        match args {
            syn::PathArguments::AngleBracketed(args) => {
                let arg = args.args.first().ok_or(syn::Error::new(
                    args.span(),
                    "Return type should be Result<Option<_>>",
                ))?;
                match arg {
                    syn::GenericArgument::Type(tp) => match tp {
                        Type::Path(tp) => {
                            let segs = &tp.path.segments;
                            let seg = segs.last().ok_or(syn::Error::new(
                                tp.span(),
                                "Return type should be Result<Option<_>>",
                            ))?;
                            if seg.ident.to_string() == "Option" {
                                let args = &seg.arguments;
                                Self::get_type_for_deserialize_from_result_args(args)
                            } else {
                                Err(syn::Error::new(
                                    seg.span(),
                                    "Return type should be Result<Option<_>>",
                                ))
                            }
                        }
                        _ => Err(syn::Error::new(
                            arg.span(),
                            "Return type should be Result<Option<_>>",
                        )),
                    },
                    _ => Err(syn::Error::new(
                        arg.span(),
                        "Return type should be Result<<Option<_>>",
                    )),
                }
            }
            _ => Err(syn::Error::new(
                args.span(),
                "Return type should be Result<_>",
            )),
        }
    }

    fn get_type_for_deserialize_from_result_args(
        args: &PathArguments,
    ) -> syn::Result<NativeOutputParams> {
        match args {
            syn::PathArguments::AngleBracketed(args) => {
                let arg = args.args.first().ok_or(syn::Error::new(
                    args.span(),
                    "Return type should be Result<_>",
                ))?;
                match arg {
                    syn::GenericArgument::Type(tp) => match tp {
                        Type::Path(tp) => {
                            let segs = &tp.path.segments;
                            let seg = segs.last().ok_or(syn::Error::new(
                                tp.span(),
                                "Return type should be Result<_>",
                            ))?;
                            let ident = &seg.ident;
                            if ident.to_string() == "Rc"
                                || ident.to_string() == "Arc"
                                || ident.to_string() == "Box"
                            {
                                if let Some(dyn_path) =
                                    Self::get_dyn_type_for_deserialize(&seg.arguments)
                                {
                                    let mut dynamic_container_path = tp.path.clone();
                                    let last_seg =
                                        dynamic_container_path.segments.last_mut().unwrap();
                                    last_seg.arguments = syn::PathArguments::None;
                                    Ok(NativeOutputParams {
                                        type_path: dyn_path,
                                        dynamic_container_path: Some(dynamic_container_path),
                                    })
                                } else {
                                    Ok(NativeOutputParams {
                                        type_path: tp.path.clone(),
                                        dynamic_container_path: None,
                                    })
                                }
                            } else {
                                Ok(NativeOutputParams {
                                    type_path: tp.path.clone(),
                                    dynamic_container_path: None,
                                })
                            }
                        }
                        _ => Err(syn::Error::new(
                            arg.span(),
                            "Return type should be Result<_>",
                        )),
                    },
                    _ => Err(syn::Error::new(
                        arg.span(),
                        "Return type should be Result<_>",
                    )),
                }
            }
            _ => Err(syn::Error::new(
                args.span(),
                "Return type should be Result<_>",
            )),
        }
    }

    fn get_dyn_type_for_deserialize(args: &PathArguments) -> Option<Path> {
        match args {
            syn::PathArguments::AngleBracketed(args) => {
                if args.args.is_empty() {
                    return None;
                }
                let arg = args.args.first().unwrap();

                match arg {
                    syn::GenericArgument::Type(tp) => match tp {
                        Type::TraitObject(to) => {
                            let type_param_bound = to.bounds.first().unwrap();
                            match type_param_bound {
                                syn::TypeParamBound::Trait(trait_bound) => {
                                    let mut path = trait_bound.path.clone();
                                    let last = path.segments.last_mut().unwrap();
                                    last.ident = format_ident!("Native{}", last.ident);
                                    Some(path)
                                }
                                _ => None,
                            }
                        }
                        _ => None,
                    },
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn imports(&self) -> proc_macro2::TokenStream {
        quote! {
            use cubenativeutils::wrappers::inner_types::InnerTypes;
            use cubenativeutils::wrappers::object::NativeStruct;
        }
    }

    fn original_trait(&self) -> proc_macro2::TokenStream {
        let service_ident = &self.ident;
        let methods = self
            .methods
            .iter()
            .map(|m| m.original_method())
            .collect::<Vec<_>>();
        // TODO Supertraits
        let static_data_method = self.static_data_method_def();
        quote! {
            pub trait #service_ident {
                #( #methods )*
                #static_data_method
            }
        }
    }

    fn static_data_method_def(&self) -> proc_macro2::TokenStream {
        if let Some(static_data_type) = &self.static_data_type {
            quote! {
                fn static_data(&self) -> &#static_data_type;
            }
        } else {
            proc_macro2::TokenStream::new()
        }
    }

    fn static_data_method_impl(&self) -> proc_macro2::TokenStream {
        if let Some(static_data_type) = &self.static_data_type {
            quote! {
                fn static_data(&self) -> &#static_data_type {
                    &self.static_data
                }
            }
        } else {
            proc_macro2::TokenStream::new()
        }
    }

    fn struct_body(&self) -> proc_macro2::TokenStream {
        let struct_ident = self.struct_ident();
        if let Some(static_data_type) = &self.static_data_type {
            quote! {
                pub struct #struct_ident<IT:InnerTypes> {
                    native_object: NativeObjectHandle<IT>,
                    static_data: #static_data_type,
                }
            }
        } else {
            quote! {
                pub struct #struct_ident<IT:InnerTypes> {
                    native_object: NativeObjectHandle<IT>,
                }
            }
        }
    }

    fn struct_ident(&self) -> Ident {
        format_ident!("Native{}", &self.ident)
    }

    fn struct_impl(&self) -> proc_macro2::TokenStream {
        let struct_ident = self.struct_ident();
        if let Some(static_data_type) = &self.static_data_type {
            quote! {
                impl<IT: InnerTypes> #struct_ident<IT> {
                    pub fn try_new(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
                        let static_data = #static_data_type::from_native(native_object.clone())?;
                        Ok(Self {native_object, static_data} )
                    }
                }
            }
        } else {
            quote! {
                impl<IT: InnerTypes> #struct_ident<IT> {
                    pub fn try_new(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
                        Ok(Self {native_object} )
                    }
                }
            }
        }
    }

    fn struct_bridge_impl(&self) -> proc_macro2::TokenStream {
        let service_ident = &self.ident;
        let struct_ident = self.struct_ident();
        let methods = self
            .methods
            .iter()
            .map(|m| m.method_impl())
            .collect::<Vec<_>>();
        let static_data_method = self.static_data_method_impl();
        quote! {


            impl<IT:InnerTypes> #service_ident for #struct_ident<IT> {
                #( #methods )*
                #static_data_method
            }
        }
    }

    fn serialization_impl(&self) -> proc_macro2::TokenStream {
        let struct_ident = self.struct_ident();
        quote! {
            impl<IT: InnerTypes> NativeSerialize<IT> for #struct_ident<IT> {

                fn to_native(&self, _context: NativeContextHolder<IT>) -> Result<NativeObjectHandle<IT>, CubeError> {
                    Ok(self.native_object.clone())
                }
            }

            impl<IT: InnerTypes> NativeDeserialize<IT> for #struct_ident<IT> {

                fn from_native(native_object: NativeObjectHandle<IT>) -> Result<Self, CubeError> {
                    Self::try_new(native_object)
                }
            }
        }
    }
}

impl NativeMethod {
    fn original_method(&self) -> proc_macro2::TokenStream {
        let &Self {
            ident,
            args,
            output,
            method_params,
            ..
        } = &self;
        if method_params.is_optional {
            let has_ident = format_ident!("has_{}", ident);

            quote! {
                fn #ident(#( #args ),*) #output;
                fn #has_ident(&self) -> Result<bool, CubeError>;
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
            args,
            output,
            output_params,
            method_params,
            ..
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
        let js_method_name = self.camel_case_name();

        let deseralization = Self::deserialization_impl(&output_params);

        match method_params.method_type {
            NativeMethodType::Call => {
                if !method_params.is_optional {
                    quote! {
                        fn #ident(#( #args ),*) #output {
                            let context_holder = NativeContextHolder::<IT>::new(self.native_object.get_context());
                            let args = vec![#( #js_args_set ),*];


                            let res = self.native_object.to_struct()?
                                .call_method(
                                    #js_method_name,
                                    args
                                )?;
                            Ok(#deseralization)
                        }
                    }
                } else {
                    let has_ident = format_ident!("has_{}", ident);
                    quote! {
                        fn #ident(#( #args ),*) #output {
                            let context_holder = NativeContextHolder::<IT>::new(self.native_object.get_context());
                            let native_struct = self.native_object.to_struct()?;
                            if native_struct.has_field(#js_method_name)? {
                                let args = vec![#( #js_args_set ),*];


                                let res = self.native_object.to_struct()?
                                    .call_method(
                                        #js_method_name,
                                        args
                                    )?;
                                Ok(Some(#deseralization))
                            } else {
                                Ok(None)
                            }
                        }
                        fn #has_ident(&self) -> Result<bool, CubeError> {
                            let context_holder = NativeContextHolder::<IT>::new(self.native_object.get_context());
                            let native_struct = self.native_object.to_struct()?;
                            native_struct.has_field(#js_method_name)
                        }
                    }
                }
            }
            NativeMethodType::Getter => {
                if !method_params.is_optional {
                    quote! {
                        fn #ident(#( #args ),*) #output {
                            let res = self.native_object.to_struct()?
                                .get_field(
                                    #js_method_name,
                                )?;

                            Ok(#deseralization)
                        }
                    }
                } else {
                    let has_ident = format_ident!("has_{}", ident);
                    quote! {
                        fn #ident(#( #args ),*) #output {
                            let native_struct = self.native_object.to_struct()?;
                            if native_struct.has_field(#js_method_name)? {
                                let res = native_struct.get_field(#js_method_name)?;
                                Ok(Some(#deseralization))
                            } else {
                                Ok(None)
                            }
                        }

                        fn #has_ident(&self) -> Result<bool, CubeError> {
                            let context_holder = NativeContextHolder::<IT>::new(self.native_object.get_context());
                            let native_struct = self.native_object.to_struct()?;
                            native_struct.has_field(#js_method_name)
                        }
                    }
                }
            }
        }
    }

    fn deserialization_impl(output_params: &NativeOutputParams) -> proc_macro2::TokenStream {
        let output_type = &output_params.type_path;

        if let Some(dynamic_container_path) = &output_params.dynamic_container_path {
            quote! {
                #dynamic_container_path::new(NativeDeserializer::deserialize::<IT, #output_type<IT>>(res)?)
            }
        } else {
            quote! {
                NativeDeserializer::deserialize::<IT, #output_type>(res)?
            }
        }
    }

    fn js_agr_set(arg: &Ident) -> proc_macro2::TokenStream {
        quote! {
            #arg.to_native(context_holder.clone())?
        }
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
}

impl ToTokens for NativeService {
    fn to_tokens(&self, tokens: &mut proc_macro2::TokenStream) {
        tokens.extend(vec![
            self.imports(),
            self.original_trait(),
            self.struct_body(),
            self.struct_impl(),
            self.struct_bridge_impl(),
            self.serialization_impl(),
        ]);
    }
}
