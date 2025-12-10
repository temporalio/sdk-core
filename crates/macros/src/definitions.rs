use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    FnArg, ImplItem, ItemImpl, Pat, ReturnType, Type, TypePath,
    parse::{Parse, ParseStream},
    spanned::Spanned,
};

pub(crate) struct ActivitiesDefinition {
    impl_block: ItemImpl,
    activities: Vec<ActivityMethod>,
}

struct ActivityMethod {
    method: syn::ImplItemFn,
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
    let is_async = method.sig.asyncness.is_some();

    // Determine if static (no self receiver) or instance (Arc<Self>)
    let is_static = match method.sig.inputs.first() {
        Some(FnArg::Receiver(_)) => false,
        Some(FnArg::Typed(pat_type)) => {
            // Check if it's `self: Arc<Self>`
            !matches!(pat_type.pat.as_ref(), Pat::Ident(ident) if ident.ident == "self")
        }
        None => true,
    };

    let input_type = extract_input_type(&method.sig)?;
    let output_type = extract_output_type(&method.sig);

    Ok(ActivityMethod {
        method: method.clone(),
        is_async,
        is_static,
        input_type,
        output_type,
    })
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
        let module_name = type_to_snake_case(impl_type);
        let module_ident = format_ident!("{}", module_name);

        // Generate the original impl block with #[activity] attributes stripped. We need that since
        // it's what's actually going to get called by the worker to run them.
        let mut cleaned_impl = self.impl_block.clone();
        for item in &mut cleaned_impl.items {
            if let ImplItem::Fn(method) = item {
                method
                    .attrs
                    .retain(|attr| !attr.path().is_ident("activity"));
            }
        }

        let activity_structs: Vec<_> = self
            .activities
            .iter()
            .map(|act| {
                let visibility = &act.method.vis;
                let struct_name = method_name_to_pascal_case(&act.method.sig.ident);
                let struct_ident = format_ident!("{}", struct_name);
                quote! {
                    #visibility struct #struct_ident;
                }
            })
            .collect();

        let activity_impls: Vec<_> = self
            .activities
            .iter()
            .map(|act| self.generate_activity_definition_impl(act, impl_type, &module_name))
            .collect();

        let implementer_impl = self.generate_activity_implementer_impl(impl_type);

        let has_only_static = if self.activities.iter().all(|a| a.is_static) {
            quote! {
                impl ::temporalio_sdk::activities::HasOnlyStaticMethods for #impl_type {}
            }
        } else {
            quote! {}
        };

        let output = quote! {
            #cleaned_impl

            pub mod #module_ident {
                use super::*;

                #(#activity_structs)*

                #(#activity_impls)*

                #implementer_impl

                #has_only_static
            }
        };

        output.into()
    }

    fn generate_activity_definition_impl(
        &self,
        activity: &ActivityMethod,
        impl_type: &Type,
        module_name: &str,
    ) -> TokenStream2 {
        let struct_name = method_name_to_pascal_case(&activity.method.sig.ident);
        let struct_ident = format_ident!("{}", struct_name);
        let method_ident = &activity.method.sig.ident;

        // Use () for input type if not specified
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

        let activity_name = format!("{}::{}", module_name, struct_name);

        let receiver_pattern = if activity.is_static {
            quote! { _receiver }
        } else {
            quote! { receiver }
        };

        let method_call = if activity.input_type.is_some() {
            if activity.is_static {
                quote! { #impl_type::#method_ident(ctx, input) }
            } else {
                quote! { #impl_type::#method_ident(receiver.unwrap(), ctx, input) }
            }
        } else if activity.is_static {
            quote! { #impl_type::#method_ident(ctx) }
        } else {
            quote! { #impl_type::#method_ident(receiver.unwrap(), ctx) }
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
            impl ::temporalio_common::ActivityDefinition for #struct_ident {
                type Input = #input_type;
                type Output = #output_type;

                fn name() -> &'static str
                where
                    Self: Sized,
                {
                    #activity_name
                }
            }

            impl ::temporalio_sdk::activities::ExecutableActivity for #struct_ident {
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

    fn generate_activity_implementer_impl(&self, impl_type: &Type) -> TokenStream2 {
        let static_activities: Vec<_> = self
            .activities
            .iter()
            .filter(|a| a.is_static)
            .map(|a| {
                let struct_name = method_name_to_pascal_case(&a.method.sig.ident);
                let struct_ident = format_ident!("{}", struct_name);
                quote! {
                    worker_options.register_activity::<#struct_ident>();
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
                    worker_options.register_activity_with_instance::<#struct_ident>(self.clone());
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
                fn register_all_static<S: ::temporalio_sdk::worker_options_builder::State>(
                    worker_options: &mut ::temporalio_sdk::WorkerOptionsBuilder<S>,
                ) {
                    #(#static_activities)*
                }

                fn register_all_instance<S: ::temporalio_sdk::worker_options_builder::State>(
                    self: ::std::sync::Arc<Self>,
                    worker_options: &mut ::temporalio_sdk::WorkerOptionsBuilder<S>,
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
