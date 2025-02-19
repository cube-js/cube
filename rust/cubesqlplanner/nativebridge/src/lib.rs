use itertools::Itertools;
use proc_macro::TokenStream;
use proc_macro2::Ident;
use quote::{format_ident, quote, ToTokens};
use syn::parse::{Parse, ParseStream};
use syn::spanned::Spanned;
use syn::token::PathSep;
use syn::LitStr;
use syn::{
    parse_macro_input, punctuated::Punctuated, FnArg, Item, Meta, Pat, Path, PathArguments,
    PathSegment, ReturnType, TraitItem, TraitItemFn, Type,
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
    pub is_vec: bool,
    pub custom_name: Option<String>,
}

impl Default for NativeMethodParams {
    fn default() -> Self {
        Self {
            method_type: NativeMethodType::Call,
            is_optional: false,
            is_vec: false,
            custom_name: None,
        }
    }
}

struct NativeOutputParams {
    type_path: Path,
    original_type: Path,
    dynamic_container_path: Option<Path>,
}

impl NativeOutputParams {
    fn type_path_if_dynamic(&self) -> Option<Path> {
        if self.dynamic_container_path.is_some() {
            Some(self.type_path.clone())
        } else {
            None
        }
    }
}

struct NativeArgumentTyped {
    ident: Ident,
    downcast_type_path: Option<Path>,
}

struct NativeMethod {
    ident: Ident,
    //custom_js_name: Option<String>
    args: Vec<FnArg>,
    typed_args: Vec<NativeArgumentTyped>,
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
                        TraitItem::Fn(method_item) => {
                            let method_params = Self::parse_method_params(method_item).unwrap();
                            let args = method_item.sig.inputs.iter().cloned().collect::<Vec<_>>();
                            let typed_args = Self::parse_method_typed_args(&args).unwrap();
                            Some(NativeMethod {
                                ident: method_item.sig.ident.clone(),
                                args,
                                typed_args,
                                output: method_item.sig.output.clone(),
                                output_params: Self::get_output_for_deserializer(
                                    &method_item.sig.output,
                                    method_params.is_optional,
                                    method_params.is_vec,
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
    fn parse_method_typed_args(args: &Vec<FnArg>) -> syn::Result<Vec<NativeArgumentTyped>> {
        args.iter()
            .filter_map(|a| match a {
                FnArg::Typed(ty) => match ty.pat.as_ref() {
                    Pat::Ident(id) => {
                        let dyn_type = Self::get_type_from_possible_dyn_type(&ty.ty);
                        match dyn_type {
                            Ok(dyn_type) => Some(Ok(NativeArgumentTyped {
                                ident: id.ident.clone(),
                                downcast_type_path: dyn_type.type_path_if_dynamic(),
                            })),
                            Err(e) => Some(Err(e)),
                        }
                    }
                    x => panic!("Unexpected pattern: {:?}", x),
                },
                FnArg::Receiver(_) => None,
            })
            .collect::<Result<Vec<_>, _>>()
    }

    fn parse_method_params(method_item: &TraitItemFn) -> syn::Result<NativeMethodParams> {
        let mut method_params = NativeMethodParams::default();

        if method_item.attrs.len() > 0 {
            for attr in method_item.attrs.iter() {
                if attr.path().is_ident("nbridge") {
                    attr.parse_nested_meta(|meta| {
                        if meta.path.is_ident("optional") {
                            method_params.is_optional = true;
                            return Ok(());
                        }
                        if meta.path.is_ident("field") {
                            method_params.method_type = NativeMethodType::Getter;
                            return Ok(());
                        }
                        if meta.path.is_ident("vec") {
                            method_params.is_vec = true;
                            return Ok(());
                        }
                        if meta.path.is_ident("rename") {
                            method_params.custom_name =
                                Some(meta.value()?.parse::<LitStr>()?.value());
                        }

                        Ok(())
                    })?;
                }
            }
        }
        Ok(method_params)
    }
    fn get_output_for_deserializer(
        tp: &ReturnType,
        optional: bool,
        vec: bool,
    ) -> syn::Result<NativeOutputParams> {
        let mut expected_type = "Result<_>".to_string();
        if optional {
            expected_type = expected_type.replace("_", "Option<_>");
        }
        if vec {
            expected_type = expected_type.replace("_", "Vec<_>");
        }
        let s = match tp {
            ReturnType::Default => Err(syn::Error::new(
                tp.span(),
                format!("Return type should be {}", expected_type),
            )),
            ReturnType::Type(_, tt) => match tt.as_ref() {
                syn::Type::Path(tp) => {
                    let segs = &tp.path.segments;
                    Self::get_deserializer_output_for_result(segs, optional, vec, &expected_type)
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
        segs: &Punctuated<PathSegment, PathSep>,
        optional: bool,
        vec: bool,
        expected_type: &str,
    ) -> syn::Result<NativeOutputParams> {
        let seg = segs.last().ok_or(syn::Error::new(
            segs.span(),
            "Return type should be Result<_>",
        ))?;
        if seg.ident.to_string() == "Result" {
            let mut args = seg.arguments.clone();
            if optional {
                args = Self::extract_output_for_nested_type(&args, "Option", expected_type)?;
            }
            if vec {
                args = Self::extract_output_for_nested_type(&args, "Vec", expected_type)?;
            }
            Self::get_type_for_deserialize_from_result_args(&args, expected_type)
        } else {
            Err(syn::Error::new(
                seg.span(),
                "Return type should be Result<_>",
            ))
        }
    }

    fn extract_output_for_nested_type(
        args: &PathArguments,
        type_to_extract: &str,
        expected_type: &str,
    ) -> syn::Result<PathArguments> {
        let error_message = format!("Return type should be {expected_type}");
        match args {
            syn::PathArguments::AngleBracketed(args) => {
                let arg = args
                    .args
                    .first()
                    .ok_or(syn::Error::new(args.span(), error_message.clone()))?;
                match arg {
                    syn::GenericArgument::Type(tp) => match tp {
                        Type::Path(tp) => {
                            let segs = &tp.path.segments;
                            let seg = segs.last().ok_or(syn::Error::new(
                                tp.span(),
                                "Return type should be Result<Option<_>>",
                            ))?;
                            if seg.ident.to_string() == type_to_extract {
                                let args = &seg.arguments;
                                Ok(args.clone())
                            } else {
                                Err(syn::Error::new(seg.span(), error_message.clone()))
                            }
                        }
                        _ => Err(syn::Error::new(arg.span(), error_message.clone())),
                    },
                    _ => Err(syn::Error::new(arg.span(), error_message.clone())),
                }
            }
            _ => Err(syn::Error::new(args.span(), error_message.clone())),
        }
    }

    fn get_type_for_deserialize_from_result_args(
        args: &PathArguments,
        expected_type: &str,
    ) -> syn::Result<NativeOutputParams> {
        let error_message = format!("Return type should be {expected_type}");
        match args {
            syn::PathArguments::AngleBracketed(args) => {
                let arg = args
                    .args
                    .first()
                    .ok_or(syn::Error::new(args.span(), error_message.clone()))?;
                match arg {
                    syn::GenericArgument::Type(tp) => Self::get_type_from_possible_dyn_type(tp),
                    _ => Err(syn::Error::new(arg.span(), error_message.clone())),
                }
            }
            _ => Err(syn::Error::new(args.span(), error_message.clone())),
        }
    }

    fn get_type_from_possible_dyn_type(tp: &Type) -> syn::Result<NativeOutputParams> {
        match tp {
            Type::Path(tp) => {
                let segs = &tp.path.segments;
                let seg = segs.last().ok_or(syn::Error::new(
                    tp.span(),
                    "Type path does not have a segment",
                ))?;
                let ident = &seg.ident;
                if ident.to_string() == "Rc"
                    || ident.to_string() == "Arc"
                    || ident.to_string() == "Box"
                {
                    if let Some(dyn_path) = Self::get_dyn_type_for_deserialize(&seg.arguments) {
                        let original_type = tp.path.clone();
                        let mut dynamic_container_path = tp.path.clone();
                        let last_seg = dynamic_container_path.segments.last_mut().unwrap();
                        last_seg.arguments = syn::PathArguments::None;
                        Ok(NativeOutputParams {
                            type_path: dyn_path,
                            original_type,
                            dynamic_container_path: Some(dynamic_container_path),
                        })
                    } else {
                        Ok(NativeOutputParams {
                            type_path: tp.path.clone(),
                            original_type: tp.path.clone(),
                            dynamic_container_path: None,
                        })
                    }
                } else {
                    Ok(NativeOutputParams {
                        type_path: tp.path.clone(),
                        original_type: tp.path.clone(),
                        dynamic_container_path: None,
                    })
                }
            }
            _ => Err(syn::Error::new(tp.span(), "Type::Path is expected")),
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
                fn as_any(self: Rc<Self>) -> Rc<dyn Any>;
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
                fn as_any(self: Rc<Self>) -> Rc<dyn Any> {
                    self.clone()
                }
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
            typed_args,
            output,
            output_params,
            method_params,
            ..
        } = &self;
        let js_args_set = typed_args
            .iter()
            .map(|arg| Self::js_agr_set(&arg.ident, &arg.downcast_type_path))
            .collect::<Vec<_>>();
        let js_method_name = method_params
            .custom_name
            .clone()
            .unwrap_or_else(|| self.camel_case_name());

        let deseralization = Self::deserialization_impl(&output_params, method_params.is_vec);

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

    fn deserialization_impl(
        output_params: &NativeOutputParams,
        is_vec: bool,
    ) -> proc_macro2::TokenStream {
        let output_type = &output_params.type_path;

        let single_deserialization = if let Some(dynamic_container_path) =
            &output_params.dynamic_container_path
        {
            quote! {
                #dynamic_container_path::new(NativeDeserializer::deserialize::<IT, #output_type<IT>>(res)?)
            }
        } else {
            quote! {
                NativeDeserializer::deserialize::<IT, #output_type>(res)?
            }
        };

        if is_vec {
            let original_type = &output_params.original_type;
            quote! {
                res.into_array()?.to_vec()?.into_iter().map(|res| -> Result<#original_type, CubeError> {
                    let r = #single_deserialization;
                    Ok(r)
                }).collect::<Result<Vec<_>, _>>()?
            }
        } else {
            single_deserialization
        }
    }

    fn js_agr_set(arg: &Ident, downcast_type_path: &Option<Path>) -> proc_macro2::TokenStream {
        if let Some(downcast_type_path) = downcast_type_path {
            quote! {
                if let Some(arg) = #arg.as_any().downcast_ref::<#downcast_type_path<IT>>() {
                    arg.to_native(context_holder.clone())?
                } else {
                    return Err(CubeError::internal(format!("Cannot dowcast arg to native type")))
                }
            }
        } else {
            quote! {
                #arg.to_native(context_holder.clone())?
            }
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
