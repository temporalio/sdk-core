use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned};
use syn::{
    Attribute, FnArg, ImplItem, ItemImpl, ReturnType, Type, TypePath,
    parse::{Parse, ParseStream},
    spanned::Spanned,
};

/// Parsed representation of a `#[workflow_methods]` impl block
pub(crate) struct WorkflowMethodsDefinition {
    impl_block: ItemImpl,
    init_method: Option<InitMethod>,
    run_method: Option<RunMethod>,
    signals: Vec<SignalMethod>,
    queries: Vec<QueryMethod>,
    updates: Vec<UpdateMethod>,
}

/// Attributes that can be applied to workflow method annotations
#[derive(Default)]
struct MethodAttributes {
    name_override: Option<syn::Expr>,
}

/// The `#[init]` method
struct InitMethod {
    method: syn::ImplItemFn,
    input_type: Option<Type>,
}

/// The `#[run]` method
struct RunMethod {
    method: syn::ImplItemFn,
    attributes: MethodAttributes,
    input_type: Option<Type>,
    output_type: Option<Type>,
}

/// A `#[signal]` method
struct SignalMethod {
    method: syn::ImplItemFn,
    attributes: MethodAttributes,
    is_async: bool,
    input_type: Option<Type>,
}

/// A `#[query]` method
struct QueryMethod {
    method: syn::ImplItemFn,
    attributes: MethodAttributes,
    input_type: Option<Type>,
    output_type: Option<Type>,
}

/// An `#[update]` method
struct UpdateMethod {
    method: syn::ImplItemFn,
    attributes: MethodAttributes,
    is_async: bool,
    input_type: Option<Type>,
    output_type: Option<Type>,
}

impl Parse for WorkflowMethodsDefinition {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let impl_block: ItemImpl = input.parse()?;
        let mut init_method = None;
        let mut run_method = None;
        let mut signals = Vec::new();
        let mut queries = Vec::new();
        let mut updates = Vec::new();

        for item in &impl_block.items {
            if let ImplItem::Fn(method) = item {
                let has_init = has_attr(&method.attrs, "init");
                let has_run = has_attr(&method.attrs, "run");
                let has_signal = has_attr(&method.attrs, "signal");
                let has_query = has_attr(&method.attrs, "query");
                let has_update = has_attr(&method.attrs, "update");

                // Count how many workflow attributes are present
                let attr_count = [has_init, has_run, has_signal, has_query, has_update]
                    .iter()
                    .filter(|&&b| b)
                    .count();

                if attr_count > 1 {
                    return Err(syn::Error::new_spanned(
                        method,
                        "A method can only have one of #[init], #[run], #[signal], #[query], or #[update]",
                    ));
                }

                if has_init {
                    if init_method.is_some() {
                        return Err(syn::Error::new_spanned(
                            method,
                            "Only one #[init] method is allowed per workflow",
                        ));
                    }
                    init_method = Some(parse_init_method(method)?);
                } else if has_run {
                    if run_method.is_some() {
                        return Err(syn::Error::new_spanned(
                            method,
                            "Only one #[run] method is allowed per workflow",
                        ));
                    }
                    run_method = Some(parse_run_method(method)?);
                } else if has_signal {
                    signals.push(parse_signal_method(method)?);
                } else if has_query {
                    queries.push(parse_query_method(method)?);
                } else if has_update {
                    updates.push(parse_update_method(method)?);
                }
            }
        }

        Ok(WorkflowMethodsDefinition {
            impl_block,
            init_method,
            run_method,
            signals,
            queries,
            updates,
        })
    }
}

fn has_attr(attrs: &[Attribute], name: &str) -> bool {
    attrs.iter().any(|attr| attr.path().is_ident(name))
}

fn extract_method_attributes(
    attrs: &[Attribute],
    attr_name: &str,
) -> syn::Result<MethodAttributes> {
    let mut method_attrs = MethodAttributes::default();

    for attr in attrs {
        if attr.path().is_ident(attr_name) && attr.meta.require_list().is_ok() {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    let value = meta.value()?;
                    let expr: syn::Expr = value.parse()?;
                    method_attrs.name_override = Some(expr);
                    Ok(())
                } else {
                    Err(meta.error(format!("unsupported {attr_name} attribute")))
                }
            })?;
        }
    }

    Ok(method_attrs)
}

fn parse_init_method(method: &syn::ImplItemFn) -> syn::Result<InitMethod> {
    // Validate: must not be async
    if method.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "#[init] methods must not be async",
        ));
    }

    // Validate: must return Self
    match &method.sig.output {
        ReturnType::Type(_, ty) => {
            if let Type::Path(TypePath { path, .. }) = &**ty {
                if !path.is_ident("Self") {
                    return Err(syn::Error::new_spanned(
                        ty,
                        "#[init] methods must return Self",
                    ));
                }
            } else {
                return Err(syn::Error::new_spanned(
                    ty,
                    "#[init] methods must return Self",
                ));
            }
        }
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &method.sig,
                "#[init] methods must return Self",
            ));
        }
    }

    // Extract input type (first parameter after ctx, if any)
    let input_type = extract_workflow_input_type(&method.sig)?;

    Ok(InitMethod {
        method: method.clone(),
        input_type,
    })
}

fn parse_run_method(method: &syn::ImplItemFn) -> syn::Result<RunMethod> {
    let attributes = extract_method_attributes(&method.attrs, "run")?;

    // Validate: must be async
    if method.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "#[run] methods must be async",
        ));
    }

    // Validate: must have &mut self receiver
    validate_mut_self_receiver(method)?;

    // Extract input type (if run takes input instead of init)
    let input_type = extract_workflow_input_type(&method.sig)?;
    let output_type = extract_workflow_result_type(&method.sig)?;

    Ok(RunMethod {
        method: method.clone(),
        attributes,
        input_type,
        output_type,
    })
}

fn parse_signal_method(method: &syn::ImplItemFn) -> syn::Result<SignalMethod> {
    let attributes = extract_method_attributes(&method.attrs, "signal")?;
    let is_async = method.sig.asyncness.is_some();

    // Validate: must have &mut self receiver
    validate_mut_self_receiver(method)?;

    let input_type = extract_handler_input_type(&method.sig)?;

    Ok(SignalMethod {
        method: method.clone(),
        attributes,
        is_async,
        input_type,
    })
}

fn parse_query_method(method: &syn::ImplItemFn) -> syn::Result<QueryMethod> {
    let attributes = extract_method_attributes(&method.attrs, "query")?;

    // Validate: must NOT be async
    if method.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "#[query] methods must not be async (queries are synchronous)",
        ));
    }

    // Validate: must have &self receiver (immutable)
    validate_immut_self_receiver(method)?;

    let input_type = extract_handler_input_type(&method.sig)?;
    let output_type = extract_simple_output_type(&method.sig);

    Ok(QueryMethod {
        method: method.clone(),
        attributes,
        input_type,
        output_type,
    })
}

fn parse_update_method(method: &syn::ImplItemFn) -> syn::Result<UpdateMethod> {
    let attributes = extract_method_attributes(&method.attrs, "update")?;
    let is_async = method.sig.asyncness.is_some();

    // Validate: must have &mut self receiver
    validate_mut_self_receiver(method)?;

    let input_type = extract_handler_input_type(&method.sig)?;
    let output_type = extract_simple_output_type(&method.sig);

    Ok(UpdateMethod {
        method: method.clone(),
        attributes,
        is_async,
        input_type,
        output_type,
    })
}

fn validate_mut_self_receiver(method: &syn::ImplItemFn) -> syn::Result<()> {
    match method.sig.inputs.first() {
        Some(FnArg::Receiver(receiver)) => {
            if receiver.reference.is_none() || receiver.mutability.is_none() {
                return Err(syn::Error::new_spanned(
                    receiver,
                    "This method must use `&mut self` as the receiver",
                ));
            }
            Ok(())
        }
        _ => Err(syn::Error::new_spanned(
            &method.sig,
            "This method must have `&mut self` as the first parameter",
        )),
    }
}

fn validate_immut_self_receiver(method: &syn::ImplItemFn) -> syn::Result<()> {
    match method.sig.inputs.first() {
        Some(FnArg::Receiver(receiver)) => {
            if receiver.reference.is_none() {
                return Err(syn::Error::new_spanned(
                    receiver,
                    "#[query] methods must use `&self` as the receiver (not `self`)",
                ));
            }
            if receiver.mutability.is_some() {
                return Err(syn::Error::new_spanned(
                    receiver,
                    "#[query] methods must use `&self` as the receiver (not `&mut self`) - queries are read-only",
                ));
            }
            Ok(())
        }
        _ => Err(syn::Error::new_spanned(
            &method.sig,
            "#[query] methods must have `&self` as the first parameter",
        )),
    }
}

/// Extract input type from workflow methods (after WfContext parameter)
fn extract_workflow_input_type(sig: &syn::Signature) -> syn::Result<Option<Type>> {
    // Find the parameter after the context parameter
    let mut found_ctx = false;
    for arg in &sig.inputs {
        if let FnArg::Typed(pat_type) = arg {
            if found_ctx {
                return Ok(Some((*pat_type.ty).clone()));
            }
            // Check if this is the context parameter (WfContext or &WfContext or &mut WfContext)
            if is_wf_context_type(&pat_type.ty) {
                found_ctx = true;
            }
        }
    }
    Ok(None)
}

/// Extract input type from handler methods (signal/query/update - after WfContext parameter)
fn extract_handler_input_type(sig: &syn::Signature) -> syn::Result<Option<Type>> {
    // Skip self, then find parameter after context
    let mut found_ctx = false;
    for arg in &sig.inputs {
        match arg {
            FnArg::Receiver(_) => continue,
            FnArg::Typed(pat_type) => {
                if found_ctx {
                    return Ok(Some((*pat_type.ty).clone()));
                }
                if is_wf_context_type(&pat_type.ty) {
                    found_ctx = true;
                }
            }
        }
    }
    Ok(None)
}

fn is_wf_context_type(ty: &Type) -> bool {
    // Check for WfContext, &WfContext, or &mut WfContext
    let inner_type = match ty {
        Type::Reference(r) => &*r.elem,
        other => other,
    };

    if let Type::Path(TypePath { path, .. }) = inner_type
        && let Some(segment) = path.segments.last()
    {
        return segment.ident == "WfContext";
    }
    false
}

/// Extract T from WorkflowResult<T> (which is Result<WfExitValue<T>, anyhow::Error>)
fn extract_workflow_result_type(sig: &syn::Signature) -> syn::Result<Option<Type>> {
    match &sig.output {
        ReturnType::Type(_, ty) => {
            // WorkflowResult<T> is an alias for Result<WfExitValue<T>, anyhow::Error>
            // We need to extract T
            if let Type::Path(TypePath { path, .. }) = &**ty
                && let Some(segment) = path.segments.last()
                && segment.ident == "WorkflowResult"
                && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
                && let Some(syn::GenericArgument::Type(output_ty)) = args.args.first()
            {
                return Ok(Some(output_ty.clone()));
            }
            // If we can't parse it as WorkflowResult, just return None
            Ok(None)
        }
        ReturnType::Default => Ok(None),
    }
}

/// Extract simple output type from return type
fn extract_simple_output_type(sig: &syn::Signature) -> Option<Type> {
    match &sig.output {
        ReturnType::Type(_, ty) => Some((**ty).clone()),
        ReturnType::Default => None,
    }
}

impl WorkflowMethodsDefinition {
    pub(crate) fn codegen(&self) -> TokenStream {
        let impl_type = &self.impl_block.self_ty;
        let impl_type_name = type_name_string(impl_type);
        let module_name = type_to_snake_case(impl_type);
        let module_ident = format_ident!("{}", module_name);

        // Generate the cleaned impl block with:
        // - workflow attributes stripped
        // - Annotated methods renamed with __ prefix
        let mut cleaned_impl = self.impl_block.clone();
        for item in &mut cleaned_impl.items {
            if let ImplItem::Fn(method) = item {
                let is_workflow_method = has_attr(&method.attrs, "init")
                    || has_attr(&method.attrs, "run")
                    || has_attr(&method.attrs, "signal")
                    || has_attr(&method.attrs, "query")
                    || has_attr(&method.attrs, "update");

                // Strip workflow-related attributes
                method.attrs.retain(|attr| {
                    !attr.path().is_ident("init")
                        && !attr.path().is_ident("run")
                        && !attr.path().is_ident("signal")
                        && !attr.path().is_ident("query")
                        && !attr.path().is_ident("update")
                });

                // Rename workflow methods with __ prefix
                if is_workflow_method {
                    let new_name = format_ident!("__{}", method.sig.ident);
                    method.sig.ident = new_name;
                }
            }
        }

        // Generate marker structs
        let mut marker_structs = Vec::new();
        let mut const_definitions = Vec::new();
        let mut trait_impls = Vec::new();

        // Run method marker struct and impls
        if let Some(ref run_method) = self.run_method {
            let struct_name = method_name_to_pascal_case(&run_method.method.sig.ident);
            let struct_ident = format_ident!("{}", struct_name);
            let method_ident = &run_method.method.sig.ident;
            let visibility = &run_method.method.vis;
            let span = run_method.method.span();

            let input_type = run_method
                .input_type
                .as_ref()
                .or(self
                    .init_method
                    .as_ref()
                    .and_then(|m| m.input_type.as_ref()))
                .map(|t| quote! { #t })
                .unwrap_or(quote! { () });

            let output_type = run_method
                .output_type
                .as_ref()
                .map(|t| quote! { #t })
                .unwrap_or(quote! { () });

            let workflow_name = if let Some(ref name_expr) = run_method.attributes.name_override {
                quote! { #name_expr }
            } else {
                quote! { #impl_type_name }
            };

            marker_structs.push(quote_spanned! { span=>
                pub struct #struct_ident;
            });

            let allow_attrs: Vec<_> = run_method
                .method
                .attrs
                .iter()
                .filter(|attr| attr.path().is_ident("allow"))
                .collect();

            const_definitions.push(quote_spanned! { span=>
                #[allow(non_upper_case_globals)]
                #(#allow_attrs)*
                #visibility const #method_ident: #module_ident::#struct_ident = #module_ident::#struct_ident;
            });

            trait_impls.push(quote! {
                impl ::temporalio_common::WorkflowDefinition for #module_ident::#struct_ident {
                    type Input = #input_type;
                    type Output = #output_type;

                    fn name() -> &'static str
                    where
                        Self: Sized,
                    {
                        #workflow_name
                    }
                }
            });
        }

        // Signal marker structs and impls
        for signal in &self.signals {
            let (structs, consts, impls) =
                self.generate_signal_code(signal, impl_type, &module_ident);
            marker_structs.push(structs);
            const_definitions.push(consts);
            trait_impls.push(impls);
        }

        // Query marker structs and impls
        for query in &self.queries {
            let (structs, consts, impls) =
                self.generate_query_code(query, impl_type, &module_ident);
            marker_structs.push(structs);
            const_definitions.push(consts);
            trait_impls.push(impls);
        }

        // Update marker structs and impls
        for update in &self.updates {
            let (structs, consts, impls) =
                self.generate_update_code(update, impl_type, &module_ident);
            marker_structs.push(structs);
            const_definitions.push(consts);
            trait_impls.push(impls);
        }

        // Generate WorkflowImplementation impl
        let workflow_impl = self.generate_workflow_implementation(impl_type, &module_ident);

        // Generate WorkflowImplementer impl
        let implementer_impl = self.generate_workflow_implementer(impl_type, &module_ident);

        // Generate impl block with consts
        let const_impl = if !const_definitions.is_empty() {
            quote! {
                impl #impl_type {
                    #(#const_definitions)*
                }
            }
        } else {
            quote! {}
        };

        let output = quote! {
            #cleaned_impl

            #const_impl

            mod #module_ident {
                #(#marker_structs)*
            }

            #(#trait_impls)*

            #workflow_impl

            #implementer_impl
        };

        output.into()
    }

    fn generate_signal_code(
        &self,
        signal: &SignalMethod,
        impl_type: &Type,
        module_ident: &syn::Ident,
    ) -> (TokenStream2, TokenStream2, TokenStream2) {
        let struct_name = method_name_to_pascal_case(&signal.method.sig.ident);
        let struct_ident = format_ident!("{}", struct_name);
        let method_ident = &signal.method.sig.ident;
        let prefixed_method = format_ident!("__{}", signal.method.sig.ident);
        let visibility = &signal.method.vis;
        let span = signal.method.span();

        let input_type = signal
            .input_type
            .as_ref()
            .map(|t| quote! { #t })
            .unwrap_or(quote! { () });

        let signal_name = if let Some(ref name_expr) = signal.attributes.name_override {
            quote! { #name_expr }
        } else {
            let name = method_ident.to_string();
            quote! { #name }
        };

        let allow_attrs: Vec<_> = signal
            .method
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("allow"))
            .collect();

        let marker_struct = quote_spanned! { span=>
            pub struct #struct_ident;
        };

        let const_def = quote_spanned! { span=>
            #[allow(non_upper_case_globals)]
            #(#allow_attrs)*
            #visibility const #method_ident: #module_ident::#struct_ident = #module_ident::#struct_ident;
        };

        let handle_body = if signal.is_async {
            let method_call = if signal.input_type.is_some() {
                quote! { self.#prefixed_method(&mut ctx, input) }
            } else {
                quote! { self.#prefixed_method(&mut ctx) }
            };
            quote! {
                let ctx = ctx.clone();
                async move {
                    let mut ctx = ctx;
                    #method_call.await
                }.boxed()
            }
        } else {
            // For sync signals, call immediately and return a completed future
            let method_call = if signal.input_type.is_some() {
                quote! { self.#prefixed_method(ctx, input) }
            } else {
                quote! { self.#prefixed_method(ctx) }
            };
            quote! {
                #method_call;
                ::std::future::ready(()).boxed()
            }
        };

        let trait_impl = quote! {
            impl ::temporalio_common::SignalDefinition for #module_ident::#struct_ident {
                type Input = #input_type;

                fn name() -> &'static str
                where
                    Self: Sized,
                {
                    #signal_name
                }
            }

            impl ::temporalio_sdk::workflows::ExecutableSignal<#module_ident::#struct_ident> for #impl_type {
                fn handle(
                    &mut self,
                    ctx: &mut ::temporalio_sdk::WfContext,
                    input: <#module_ident::#struct_ident as ::temporalio_common::SignalDefinition>::Input,
                ) -> ::futures_util::future::BoxFuture<'_, ()> {
                    use ::futures_util::FutureExt;
                    #handle_body
                }
            }
        };

        (marker_struct, const_def, trait_impl)
    }

    fn generate_query_code(
        &self,
        query: &QueryMethod,
        impl_type: &Type,
        module_ident: &syn::Ident,
    ) -> (TokenStream2, TokenStream2, TokenStream2) {
        let struct_name = method_name_to_pascal_case(&query.method.sig.ident);
        let struct_ident = format_ident!("{}", struct_name);
        let method_ident = &query.method.sig.ident;
        let prefixed_method = format_ident!("__{}", query.method.sig.ident);
        let visibility = &query.method.vis;
        let span = query.method.span();

        let input_type = query
            .input_type
            .as_ref()
            .map(|t| quote! { #t })
            .unwrap_or(quote! { () });

        let output_type = query
            .output_type
            .as_ref()
            .map(|t| quote! { #t })
            .unwrap_or(quote! { () });

        let query_name = if let Some(ref name_expr) = query.attributes.name_override {
            quote! { #name_expr }
        } else {
            let name = method_ident.to_string();
            quote! { #name }
        };

        let allow_attrs: Vec<_> = query
            .method
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("allow"))
            .collect();

        let marker_struct = quote_spanned! { span=>
            pub struct #struct_ident;
        };

        let const_def = quote_spanned! { span=>
            #[allow(non_upper_case_globals)]
            #(#allow_attrs)*
            #visibility const #method_ident: #module_ident::#struct_ident = #module_ident::#struct_ident;
        };

        let method_call = if query.input_type.is_some() {
            quote! { self.#prefixed_method(ctx, input) }
        } else {
            quote! { self.#prefixed_method(ctx) }
        };

        let trait_impl = quote! {
            impl ::temporalio_common::QueryDefinition for #module_ident::#struct_ident {
                type Input = #input_type;
                type Output = #output_type;

                fn name() -> &'static str
                where
                    Self: Sized,
                {
                    #query_name
                }
            }

            impl ::temporalio_sdk::workflows::ExecutableQuery<#module_ident::#struct_ident> for #impl_type {
                fn handle(
                    &self,
                    ctx: &::temporalio_sdk::WfContext,
                    input: <#module_ident::#struct_ident as ::temporalio_common::QueryDefinition>::Input,
                ) -> <#module_ident::#struct_ident as ::temporalio_common::QueryDefinition>::Output {
                    #method_call
                }
            }
        };

        (marker_struct, const_def, trait_impl)
    }

    fn generate_update_code(
        &self,
        update: &UpdateMethod,
        impl_type: &Type,
        module_ident: &syn::Ident,
    ) -> (TokenStream2, TokenStream2, TokenStream2) {
        let struct_name = method_name_to_pascal_case(&update.method.sig.ident);
        let struct_ident = format_ident!("{}", struct_name);
        let method_ident = &update.method.sig.ident;
        let prefixed_method = format_ident!("__{}", update.method.sig.ident);
        let visibility = &update.method.vis;
        let span = update.method.span();

        let input_type = update
            .input_type
            .as_ref()
            .map(|t| quote! { #t })
            .unwrap_or(quote! { () });

        let output_type = update
            .output_type
            .as_ref()
            .map(|t| quote! { #t })
            .unwrap_or(quote! { () });

        let update_name = if let Some(ref name_expr) = update.attributes.name_override {
            quote! { #name_expr }
        } else {
            let name = method_ident.to_string();
            quote! { #name }
        };

        let allow_attrs: Vec<_> = update
            .method
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("allow"))
            .collect();

        let marker_struct = quote_spanned! { span=>
            pub struct #struct_ident;
        };

        let const_def = quote_spanned! { span=>
            #[allow(non_upper_case_globals)]
            #(#allow_attrs)*
            #visibility const #method_ident: #module_ident::#struct_ident = #module_ident::#struct_ident;
        };

        let handle_body = if update.is_async {
            let method_call = if update.input_type.is_some() {
                quote! { self.#prefixed_method(&mut ctx, input) }
            } else {
                quote! { self.#prefixed_method(&mut ctx) }
            };
            quote! {
                let ctx = ctx.clone();
                async move {
                    let mut ctx = ctx;
                    #method_call.await
                }.boxed()
            }
        } else {
            // For sync updates, call immediately and return a completed future with the result
            let method_call = if update.input_type.is_some() {
                quote! { self.#prefixed_method(ctx, input) }
            } else {
                quote! { self.#prefixed_method(ctx) }
            };
            quote! {
                let result = #method_call;
                ::std::future::ready(result).boxed()
            }
        };

        let trait_impl = quote! {
            impl ::temporalio_common::UpdateDefinition for #module_ident::#struct_ident {
                type Input = #input_type;
                type Output = #output_type;

                fn name() -> &'static str
                where
                    Self: Sized,
                {
                    #update_name
                }
            }

            impl ::temporalio_sdk::workflows::ExecutableUpdate<#module_ident::#struct_ident> for #impl_type {
                fn handle(
                    &mut self,
                    ctx: &mut ::temporalio_sdk::WfContext,
                    input: <#module_ident::#struct_ident as ::temporalio_common::UpdateDefinition>::Input,
                ) -> ::futures_util::future::BoxFuture<'_, <#module_ident::#struct_ident as ::temporalio_common::UpdateDefinition>::Output> {
                    use ::futures_util::FutureExt;
                    #handle_body
                }
            }
        };

        (marker_struct, const_def, trait_impl)
    }

    fn generate_workflow_implementation(
        &self,
        impl_type: &Type,
        module_ident: &syn::Ident,
    ) -> TokenStream2 {
        let run_method = match &self.run_method {
            Some(r) => r,
            None => {
                // If no run method, we can't implement WorkflowImplementation
                // This will be caught at compile time when trying to use the workflow
                return quote! {};
            }
        };

        let run_struct_name = method_name_to_pascal_case(&run_method.method.sig.ident);
        let run_struct_ident = format_ident!("{}", run_struct_name);
        let prefixed_run = format_ident!("__{}", run_method.method.sig.ident);

        // Determine if init takes input or run takes input
        let init_has_input = self
            .init_method
            .as_ref()
            .map(|m| m.input_type.is_some())
            .unwrap_or(false);
        let run_has_input = run_method.input_type.is_some();

        let init_body = if let Some(ref init_method) = self.init_method {
            let prefixed_init = format_ident!("__{}", init_method.method.sig.ident);
            if init_has_input {
                // Signature: fn new(ctx: &WfContext, input: T) -> Self
                quote! {
                    let deserialized: <Self::Run as ::temporalio_common::WorkflowDefinition>::Input =
                        converter.from_payload(&::temporalio_common::data_converters::SerializationContext::Workflow, input)?;
                    Ok(#impl_type::#prefixed_init(ctx, deserialized))
                }
            } else {
                quote! {
                    Ok(#impl_type::#prefixed_init(ctx))
                }
            }
        } else {
            // Use Default
            quote! {
                Ok(Self::default())
            }
        };

        let run_call = if run_has_input {
            quote! { self.#prefixed_run(&mut ctx, deserialized).await }
        } else {
            quote! { self.#prefixed_run(&mut ctx).await }
        };

        let run_impl_body = if run_has_input {
            quote! {
                use ::futures_util::FutureExt;
                use ::temporalio_common::data_converters::GenericPayloadConverter;
                let converter = ::temporalio_common::data_converters::PayloadConverter::serde_json();
                let deserialized: <Self::Run as ::temporalio_common::WorkflowDefinition>::Input =
                    match converter.from_payload(&::temporalio_common::data_converters::SerializationContext::Workflow, todo!("get input payload")) {
                        Ok(v) => v,
                        Err(e) => return ::std::boxed::Box::pin(async move { Err(e.into()) }),
                    };
                async move {
                    let mut ctx = ctx;
                    let result = #run_call;
                    match result {
                        Ok(exit_value) => {
                            match exit_value {
                                ::temporalio_sdk::WfExitValue::Normal(v) => {
                                    converter.to_payload(&::temporalio_common::data_converters::SerializationContext::Workflow, &v)
                                        .map_err(Into::into)
                                }
                                ::temporalio_sdk::WfExitValue::ContinueAsNew(_) |
                                ::temporalio_sdk::WfExitValue::Cancelled |
                                ::temporalio_sdk::WfExitValue::Evicted => {
                                    todo!("Handle non-normal exit values")
                                }
                            }
                        }
                        Err(e) => Err(e.into()),
                    }
                }.boxed()
            }
        } else {
            quote! {
                use ::futures_util::FutureExt;
                use ::temporalio_common::data_converters::GenericPayloadConverter;
                let converter = ::temporalio_common::data_converters::PayloadConverter::serde_json();
                async move {
                    let mut ctx = ctx;
                    let result = #run_call;
                    match result {
                        Ok(exit_value) => {
                            match exit_value {
                                ::temporalio_sdk::WfExitValue::Normal(v) => {
                                    converter.to_payload(&::temporalio_common::data_converters::SerializationContext::Workflow, &v)
                                        .map_err(Into::into)
                                }
                                ::temporalio_sdk::WfExitValue::ContinueAsNew(_) |
                                ::temporalio_sdk::WfExitValue::Cancelled |
                                ::temporalio_sdk::WfExitValue::Evicted => {
                                    todo!("Handle non-normal exit values")
                                }
                            }
                        }
                        Err(e) => Err(e.into()),
                    }
                }.boxed()
            }
        };

        quote! {
            impl ::temporalio_sdk::workflows::WorkflowImplementation for #impl_type {
                type Run = #module_ident::#run_struct_ident;

                fn init(
                    input: ::temporalio_common::protos::temporal::api::common::v1::Payload,
                    converter: &::temporalio_common::data_converters::PayloadConverter,
                    ctx: &::temporalio_sdk::WfContext,
                ) -> Result<Self, ::temporalio_common::data_converters::PayloadConversionError> {
                    use ::temporalio_common::data_converters::GenericPayloadConverter;
                    #init_body
                }

                fn run(
                    &mut self,
                    ctx: ::temporalio_sdk::WfContext,
                ) -> ::futures_util::future::BoxFuture<'_, Result<::temporalio_common::protos::temporal::api::common::v1::Payload, ::temporalio_sdk::workflows::WorkflowError>> {
                    #run_impl_body
                }
            }
        }
    }

    fn generate_workflow_implementer(
        &self,
        impl_type: &Type,
        module_ident: &syn::Ident,
    ) -> TokenStream2 {
        let mut registrations = Vec::new();

        // Register run
        if self.run_method.is_some() {
            registrations.push(quote! {
                defs.register_workflow_run::<Self>();
            });
        }

        // Register signals
        for signal in &self.signals {
            let struct_name = method_name_to_pascal_case(&signal.method.sig.ident);
            let struct_ident = format_ident!("{}", struct_name);
            registrations.push(quote! {
                defs.register_signal::<Self, #module_ident::#struct_ident>();
            });
        }

        // Register queries
        for query in &self.queries {
            let struct_name = method_name_to_pascal_case(&query.method.sig.ident);
            let struct_ident = format_ident!("{}", struct_name);
            registrations.push(quote! {
                defs.register_query::<Self, #module_ident::#struct_ident>();
            });
        }

        // Register updates
        for update in &self.updates {
            let struct_name = method_name_to_pascal_case(&update.method.sig.ident);
            let struct_ident = format_ident!("{}", struct_name);
            registrations.push(quote! {
                defs.register_update::<Self, #module_ident::#struct_ident>();
            });
        }

        quote! {
            impl ::temporalio_sdk::workflows::WorkflowImplementer for #impl_type {
                fn register_all(defs: &mut ::temporalio_sdk::workflows::WorkflowDefinitions) {
                    #(#registrations)*
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
