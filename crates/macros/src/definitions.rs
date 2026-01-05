use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned};
use syn::{
    Attribute, FnArg, ImplItem, ItemImpl, ReturnType, Type, TypePath,
    parse::{Parse, ParseStream},
    spanned::Spanned,
};

pub(crate) struct ActivitiesDefinition {
    impl_block: ItemImpl,
    activities: Vec<ActivityMethod>,
}

#[derive(Default)]
struct ActivityAttributes {
    name_override: Option<syn::Expr>,
}

struct ActivityMethod {
    method: syn::ImplItemFn,
    attributes: ActivityAttributes,
    is_async: bool,
    is_static: bool,
    input_type: Option<Type>,
    output_type: Option<Type>,
}

impl Parse for ActivitiesDefinition {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let impl_block: ItemImpl = input.parse()?;
        let mut activities = Vec::new();

        // Extract methods marked with #[activity]
        for item in &impl_block.items {
            if let ImplItem::Fn(method) = item {
                let has_activity_attr = method
                    .attrs
                    .iter()
                    .any(|attr| attr.path().is_ident("activity"));

                if has_activity_attr {
                    let activity = parse_activity_method(method)?;
                    activities.push(activity);
                }
            }
        }

        Ok(ActivitiesDefinition {
            impl_block,
            activities,
        })
    }
}

fn parse_activity_method(method: &syn::ImplItemFn) -> syn::Result<ActivityMethod> {
    let attributes = extract_activity_attributes(method.attrs.as_slice())?;
    let is_async = method.sig.asyncness.is_some();

    // Determine if static (no self receiver) or instance (Arc<Self>)
    let is_static = match method.sig.inputs.first() {
        Some(FnArg::Receiver(receiver)) => {
            if receiver.colon_token.is_some() {
                validate_arc_self_type(&receiver.ty)?;
                false
            } else {
                return Err(syn::Error::new_spanned(
                    receiver,
                    "Activity methods with instance state must use `self: Arc<Self>` as the \
                     receiver, not `self`, `&self`, or `&mut self`",
                ));
            }
        }
        Some(FnArg::Typed(_)) | None => true,
    };

    let input_type = extract_input_type(&method.sig)?;
    let output_type = extract_output_type(&method.sig);

    Ok(ActivityMethod {
        method: method.clone(),
        attributes,
        is_async,
        is_static,
        input_type,
        output_type,
    })
}

fn extract_activity_attributes(attrs: &[Attribute]) -> syn::Result<ActivityAttributes> {
    let mut activity_attributes = ActivityAttributes::default();

    for attr in attrs {
        if attr.path().is_ident("activity") && attr.meta.require_list().is_ok() {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    let value = meta.value()?;
                    let expr: syn::Expr = value.parse()?;
                    activity_attributes.name_override = Some(expr);
                    Ok(())
                } else {
                    Err(meta.error("unsupported activity attribute"))
                }
            })?;
        }
    }

    Ok(activity_attributes)
}

fn validate_arc_self_type(ty: &Type) -> syn::Result<()> {
    let expected: Type = syn::parse_quote!(Arc<Self>);

    if let (Type::Path(actual_path), Type::Path(expected_path)) = (ty, &expected)
        && let (Some(actual_seg), Some(expected_seg)) = (
            actual_path.path.segments.last(),
            expected_path.path.segments.last(),
        )
        && actual_seg == expected_seg
    {
        return Ok(());
    }

    Err(syn::Error::new_spanned(
        ty,
        "Instance activity methods must use `self: Arc<Self>` as the receiver type",
    ))
}

fn extract_input_type(sig: &syn::Signature) -> syn::Result<Option<Type>> {
    // Find the parameter after ActivityContext, if any
    let mut found_ctx = false;
    for arg in &sig.inputs {
        if let FnArg::Typed(pat_type) = arg {
            if found_ctx {
                return Ok(Some((*pat_type.ty).clone()));
            }
            if let Type::Path(type_path) = &*pat_type.ty
                && type_path
                    .path
                    .segments
                    .last()
                    .map(|s| s.ident == "ActivityContext")
                    .unwrap_or(false)
            {
                found_ctx = true;
            }
        }
    }
    if !found_ctx {
        return Err(syn::Error::new(
            sig.inputs.span(),
            "Activity functions must have an ActivityContext parameter as either the first \
             parameter, or the second after `self: Arc<Self>`.",
        ));
    }
    Ok(None)
}

fn extract_output_type(sig: &syn::Signature) -> Option<Type> {
    match &sig.output {
        ReturnType::Type(_, ty) => {
            // Extract T from Result<T, ActivityError>
            if let Type::Path(TypePath { path, .. }) = &**ty
                && let Some(segment) = path.segments.last()
                && segment.ident == "Result"
                && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
                && let Some(syn::GenericArgument::Type(output_ty)) = args.args.first()
            {
                return Some(output_ty.clone());
            }
            // If it's not a Result type, assume it returns the type directly
            Some((**ty).clone())
        }
        ReturnType::Default => None,
    }
}

impl ActivitiesDefinition {
    pub(crate) fn codegen(&self) -> TokenStream {
        let impl_type = &self.impl_block.self_ty;
        let impl_type_name = type_name_string(impl_type);
        let module_name = type_to_snake_case(impl_type);
        let module_ident = format_ident!("{}", module_name);

        // Generate the original impl block with:
        // - #[activity] attributes stripped
        // - Activity methods renamed with __ prefix
        let mut cleaned_impl = self.impl_block.clone();
        for item in &mut cleaned_impl.items {
            if let ImplItem::Fn(method) = item {
                let is_activity = method
                    .attrs
                    .iter()
                    .any(|attr| attr.path().is_ident("activity"));

                method
                    .attrs
                    .retain(|attr| !attr.path().is_ident("activity"));

                // Rename activity methods with __ prefix
                if is_activity {
                    let new_name = format_ident!("__{}", method.sig.ident);
                    method.sig.ident = new_name;
                }
            }
        }

        // Generate marker structs (inside module, no external references)
        let activity_structs: Vec<_> = self
            .activities
            .iter()
            .map(|act| {
                let visibility = match &act.method.vis {
                    syn::Visibility::Inherited => &syn::parse_quote!(pub(super)),
                    o => o,
                };

                let struct_name = method_name_to_pascal_case(&act.method.sig.ident);
                let struct_ident = format_ident!("{}", struct_name);
                let span = act.method.span();
                quote_spanned! { span=>
                    #visibility struct #struct_ident;
                }
            })
            .collect();

        // Generate consts in impl block pointing to marker structs
        let activity_consts: Vec<_> = self
            .activities
            .iter()
            .map(|act| {
                let visibility = &act.method.vis;
                let method_ident = &act.method.sig.ident;
                let struct_name = method_name_to_pascal_case(&act.method.sig.ident);
                let struct_ident = format_ident!("{}", struct_name);
                let span = act.method.span();
                // Copy #[allow(...)] attributes from the method to the const
                let allow_attrs: Vec<_> = act
                    .method
                    .attrs
                    .iter()
                    .filter(|attr| attr.path().is_ident("allow"))
                    .collect();
                quote_spanned! { span=>
                    #[allow(non_upper_case_globals)]
                    #(#allow_attrs)*
                    #visibility const #method_ident: #module_ident::#struct_ident = #module_ident::#struct_ident;
                }
            })
            .collect();

        // Generate run methods on marker structs (outside module to reference impl_type)
        let run_impls: Vec<_> = self
            .activities
            .iter()
            .map(|act| self.generate_run_impl(act, impl_type, &module_ident))
            .collect();

        // Generate ActivityDefinition and ExecutableActivity impls (outside module)
        let activity_impls: Vec<_> = self
            .activities
            .iter()
            .map(|act| {
                self.generate_activity_definition_impl(
                    act,
                    impl_type,
                    &impl_type_name,
                    &module_ident,
                )
            })
            .collect();

        let implementer_impl = self.generate_activity_implementer_impl(impl_type, &module_ident);

        let has_only_static = if self.activities.iter().all(|a| a.is_static) {
            quote! {
                impl ::temporalio_sdk::activities::HasOnlyStaticMethods for #impl_type {}
            }
        } else {
            quote! {}
        };

        // Generate impl block with consts
        let const_impl = quote! {
            impl #impl_type {
                #(#activity_consts)*
            }
        };

        let output = quote! {
            #cleaned_impl

            #const_impl

            // Module contains only the marker structs (no use super::*)
            mod #module_ident {
                #(#activity_structs)*
            }

            // Run methods, trait impls are outside the module
            #(#run_impls)*

            #(#activity_impls)*

            #implementer_impl

            #has_only_static
        };

        output.into()
    }

    fn generate_run_impl(
        &self,
        activity: &ActivityMethod,
        impl_type: &Type,
        module_ident: &syn::Ident,
    ) -> TokenStream2 {
        let struct_name = method_name_to_pascal_case(&activity.method.sig.ident);
        let struct_ident = format_ident!("{}", struct_name);
        let prefixed_method = format_ident!("__{}", activity.method.sig.ident);

        let input_type = activity
            .input_type
            .as_ref()
            .map(|t| quote! { #t })
            .unwrap_or(quote! { () });
        let output_type = activity
            .output_type
            .as_ref()
            .map(|t| quote! { #t })
            .unwrap_or(quote! { () });

        // Build the parameters and call based on static vs instance and input
        let (params, method_call) = if activity.is_static {
            let params = if activity.input_type.is_some() {
                quote! { self, ctx: ::temporalio_sdk::activities::ActivityContext, input: #input_type }
            } else {
                quote! { self, ctx: ::temporalio_sdk::activities::ActivityContext }
            };
            let call = if activity.input_type.is_some() {
                quote! { #impl_type::#prefixed_method(ctx, input) }
            } else {
                quote! { #impl_type::#prefixed_method(ctx) }
            };
            (params, call)
        } else {
            let params = if activity.input_type.is_some() {
                quote! { self, instance: ::std::sync::Arc<#impl_type>, ctx: ::temporalio_sdk::activities::ActivityContext, input: #input_type }
            } else {
                quote! { self, instance: ::std::sync::Arc<#impl_type>, ctx: ::temporalio_sdk::activities::ActivityContext }
            };
            let call = if activity.input_type.is_some() {
                quote! { #impl_type::#prefixed_method(instance, ctx, input) }
            } else {
                quote! { #impl_type::#prefixed_method(instance, ctx) }
            };
            (params, call)
        };

        let return_type =
            quote! { Result<#output_type, ::temporalio_sdk::activities::ActivityError> };

        // If the method returns void (no return type), wrap with Ok(())
        let result_wrapper = if activity.output_type.is_none() {
            quote! { ; Ok(()) }
        } else {
            quote! {}
        };

        // Common methods for all marker structs
        let common_methods = quote! {
            /// Returns the activity name (delegates to ActivityDefinition::name())
            pub fn name(&self) -> &'static str {
                <Self as ::temporalio_common::ActivityDefinition>::name()
            }
        };

        if activity.is_async {
            quote! {
                impl #module_ident::#struct_ident {
                    #common_methods

                    pub async fn run(#params) -> #return_type {
                        #method_call.await #result_wrapper
                    }
                }
            }
        } else {
            quote! {
                impl #module_ident::#struct_ident {
                    #common_methods

                    pub fn run(#params) -> #return_type {
                        #method_call #result_wrapper
                    }
                }
            }
        }
    }

    fn generate_activity_definition_impl(
        &self,
        activity: &ActivityMethod,
        impl_type: &Type,
        impl_type_name: &str,
        module_ident: &syn::Ident,
    ) -> TokenStream2 {
        let struct_name = method_name_to_pascal_case(&activity.method.sig.ident);
        let struct_ident = format_ident!("{}", struct_name);
        let prefixed_method = format_ident!("__{}", activity.method.sig.ident);

        let input_type = activity
            .input_type
            .as_ref()
            .map(|t| quote! { #t })
            .unwrap_or(quote! { () });
        let output_type = &activity
            .output_type
            .as_ref()
            .map(|t| quote! { #t })
            .unwrap_or(quote! { () });

        let activity_name = if let Some(ref name_expr) = activity.attributes.name_override {
            quote! { #name_expr }
        } else {
            let default_name = format!("{}::{}", impl_type_name, activity.method.sig.ident);
            quote! { #default_name }
        };

        let receiver_pattern = if activity.is_static {
            quote! { _receiver }
        } else {
            quote! { receiver }
        };

        let method_call = if activity.input_type.is_some() {
            if activity.is_static {
                quote! { #impl_type::#prefixed_method(ctx, input) }
            } else {
                quote! { #impl_type::#prefixed_method(receiver.unwrap(), ctx, input) }
            }
        } else if activity.is_static {
            quote! { #impl_type::#prefixed_method(ctx) }
        } else {
            quote! { #impl_type::#prefixed_method(receiver.unwrap(), ctx) }
        };

        // Add input parameter to execute signature only if needed
        let input_param = if activity.input_type.is_some() {
            quote! { input: Self::Input, }
        } else {
            quote! { _input: Self::Input, }
        };

        let result_returner = if activity.output_type.is_none() {
            quote! {; Ok(()) }
        } else {
            quote! {}
        };
        let execute_body = if activity.is_async {
            quote! {
                async move { #method_call.await #result_returner }.boxed()
            }
        } else {
            quote! {
                tokio::task::spawn_blocking(move || { #method_call #result_returner })
                    .map(|jh| match jh {
                        Err(err) => Err(::temporalio_sdk::activities::ActivityError::from(err)),
                        Ok(v) => v,
                    })
                    .boxed()
            }
        };

        quote! {
            impl ::temporalio_common::ActivityDefinition for #module_ident::#struct_ident {
                type Input = #input_type;
                type Output = #output_type;

                fn name() -> &'static str
                where
                    Self: Sized,
                {
                    #activity_name
                }
            }

            impl ::temporalio_sdk::activities::ExecutableActivity for #module_ident::#struct_ident {
                type Implementer = #impl_type;

                fn execute(
                    #receiver_pattern: Option<::std::sync::Arc<Self::Implementer>>,
                    ctx: ::temporalio_sdk::activities::ActivityContext,
                    #input_param
                ) -> ::futures::future::BoxFuture<'static,
                    Result<Self::Output, ::temporalio_sdk::activities::ActivityError>>
                {
                    use ::futures::FutureExt;

                    #execute_body
                }
            }
        }
    }

    fn generate_activity_implementer_impl(
        &self,
        impl_type: &Type,
        module_ident: &syn::Ident,
    ) -> TokenStream2 {
        let static_activities: Vec<_> = self
            .activities
            .iter()
            .filter(|a| a.is_static)
            .map(|a| {
                let struct_name = method_name_to_pascal_case(&a.method.sig.ident);
                let struct_ident = format_ident!("{}", struct_name);
                quote! {
                    defs.register_activity::<#module_ident::#struct_ident>();
                }
            })
            .collect();

        let instance_activities: Vec<_> = self
            .activities
            .iter()
            .filter(|a| !a.is_static)
            .map(|a| {
                let struct_name = method_name_to_pascal_case(&a.method.sig.ident);
                let struct_ident = format_ident!("{}", struct_name);
                quote! {
                    defs.register_activity_with_instance::<#module_ident::#struct_ident>(self.clone());
                }
            })
            .collect();

        let register_instance_body = if instance_activities.is_empty() {
            quote! {}
        } else {
            quote! { #(#instance_activities)* }
        };

        quote! {
            impl ::temporalio_sdk::activities::ActivityImplementer for #impl_type {
                fn register_all_static(
                    defs: &mut ::temporalio_sdk::activities::ActivityDefinitions,
                ) {
                    #(#static_activities)*
                }

                fn register_all_instance(
                    self: ::std::sync::Arc<Self>,
                    defs: &mut ::temporalio_sdk::activities::ActivityDefinitions,
                ) {
                    #register_instance_body
                }
            }
        }
    }
}

fn type_to_snake_case(ty: &Type) -> String {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
    {
        return ident_to_snake_case(&segment.ident.to_string());
    }
    "unknown".to_string()
}

fn ident_to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                result.push('_');
            }
            result.push(ch.to_lowercase().next().unwrap());
        } else {
            result.push(ch);
        }
    }
    result
}

fn method_name_to_pascal_case(ident: &syn::Ident) -> String {
    let s = ident.to_string();
    let mut result = String::new();
    let mut capitalize_next = true;

    for ch in s.chars() {
        if ch == '_' {
            capitalize_next = true;
        } else if capitalize_next {
            result.push(ch.to_uppercase().next().unwrap());
            capitalize_next = false;
        } else {
            result.push(ch);
        }
    }
    result
}

fn type_name_string(ty: &Type) -> String {
    if let Type::Path(type_path) = ty
        && let Some(segment) = type_path.path.segments.last()
    {
        return segment.ident.to_string();
    }
    panic!("Cannot extract type name from impl block - expected a simple type path");
}
