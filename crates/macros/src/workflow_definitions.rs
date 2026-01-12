use crate::macro_utils::{
    generate_const_definition, generate_marker_struct, method_name_to_pascal_case,
    type_name_string, type_to_snake_case,
};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Attribute, FnArg, ImplItem, ItemImpl, ReturnType, Type, TypePath,
    parse::{Parse, ParseStream},
};

/// Convert an optional type to a quoted token stream, defaulting to `()` if None.
fn type_to_tokens(ty: Option<&Type>) -> TokenStream2 {
    ty.map(|t| quote! { #t }).unwrap_or(quote! { () })
}

/// Common data extracted from handler methods (signal, query, update)
struct HandlerCodegenInfo {
    struct_ident: syn::Ident,
    prefixed_method: syn::Ident,
    input_type_tokens: TokenStream2,
    handler_name: TokenStream2,
    marker_struct: TokenStream2,
    const_def: TokenStream2,
}

impl HandlerCodegenInfo {
    fn new(
        method: &syn::ImplItemFn,
        attributes: &MethodAttributes,
        input_type: Option<&Type>,
        module_ident: &syn::Ident,
    ) -> Self {
        let struct_name = method_name_to_pascal_case(&method.sig.ident);
        let struct_ident = format_ident!("{}", struct_name);
        let prefixed_method = format_ident!("__{}", method.sig.ident);
        let input_type_tokens = type_to_tokens(input_type);
        let handler_name = {
            let method_ident: &syn::Ident = &method.sig.ident;
            if let Some(ref name_expr) = attributes.name_override {
                quote! { #name_expr }
            } else {
                let name = method_ident.to_string();
                quote! { #name }
            }
        };
        let marker_struct = generate_marker_struct(method);
        let const_def = generate_const_definition(method, module_ident);

        Self {
            struct_ident,
            prefixed_method,
            input_type_tokens,
            handler_name,
            marker_struct,
            const_def,
        }
    }
}

/// Generate a method call with or without input parameter
fn generate_method_call(prefixed_method: &syn::Ident, has_input: bool) -> TokenStream2 {
    if has_input {
        quote! { self.#prefixed_method(ctx, input) }
    } else {
        quote! { self.#prefixed_method(ctx) }
    }
}

/// Generate a registration statement for a handler type
fn generate_handler_registration(
    method: &syn::ImplItemFn,
    module_ident: &syn::Ident,
    register_fn: &str,
) -> TokenStream2 {
    let struct_name = method_name_to_pascal_case(&method.sig.ident);
    let struct_ident = format_ident!("{}", struct_name);
    let register_ident = format_ident!("{}", register_fn);
    quote! {
        defs.#register_ident::<Self, #module_ident::#struct_ident>();
    }
}

/// Generate an async handler body that clones ctx and awaits the method call
fn generate_async_handler_body(prefixed_method: &syn::Ident, has_input: bool) -> TokenStream2 {
    let method_call = if has_input {
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
}

/// Generate a sync handler body that calls immediately and returns a ready future
fn generate_sync_handler_body(
    prefixed_method: &syn::Ident,
    has_input: bool,
    returns_value: bool,
) -> TokenStream2 {
    let method_call = if has_input {
        quote! { self.#prefixed_method(ctx, input) }
    } else {
        quote! { self.#prefixed_method(ctx) }
    };

    if returns_value {
        quote! {
            let result = #method_call;
            ::std::future::ready(result).boxed()
        }
    } else {
        quote! {
            #method_call;
            ::std::future::ready(()).boxed()
        }
    }
}

/// Parsed representation of a `#[workflow_methods]` impl block
pub(crate) struct WorkflowMethodsDefinition {
    impl_block: ItemImpl,
    init_method: Option<InitMethod>,
    run_method: RunMethod,
    signals: Vec<SignalMethod>,
    queries: Vec<QueryMethod>,
    updates: Vec<UpdateMethod>,
}

#[derive(Default)]
struct MethodAttributes {
    name_override: Option<syn::Expr>,
}

struct InitMethod {
    method: syn::ImplItemFn,
    input_type: Option<Type>,
}

struct RunMethod {
    method: syn::ImplItemFn,
    attributes: MethodAttributes,
    input_type: Option<Type>,
    output_type: Option<Type>,
}

struct SignalMethod {
    method: syn::ImplItemFn,
    attributes: MethodAttributes,
    is_async: bool,
    input_type: Option<Type>,
}

struct QueryMethod {
    method: syn::ImplItemFn,
    attributes: MethodAttributes,
    input_type: Option<Type>,
    output_type: Option<Type>,
}

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

        let run_method = run_method.ok_or_else(|| {
            syn::Error::new_spanned(
                &impl_block,
                "#[workflow_methods] requires exactly one #[run] method",
            )
        })?;

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
    let error_msg = "#[init] methods must return Self";
    match &method.sig.output {
        ReturnType::Type(_, ty) => {
            if let Type::Path(TypePath { path, .. }) = &**ty {
                if !path.is_ident("Self") {
                    return Err(syn::Error::new_spanned(ty, error_msg));
                }
            } else {
                return Err(syn::Error::new_spanned(ty, error_msg));
            }
        }
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(&method.sig, error_msg));
        }
    }
    let input_type = extract_input_type(&method.sig)?;

    Ok(InitMethod {
        method: method.clone(),
        input_type,
    })
}

fn parse_run_method(method: &syn::ImplItemFn) -> syn::Result<RunMethod> {
    let attributes = extract_method_attributes(&method.attrs, "run")?;

    if method.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "#[run] methods must be async",
        ));
    }

    validate_mut_self_receiver(method)?;

    let input_type = extract_input_type(&method.sig)?;
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

    validate_mut_self_receiver(method)?;

    let input_type = extract_input_type(&method.sig)?;

    Ok(SignalMethod {
        method: method.clone(),
        attributes,
        is_async,
        input_type,
    })
}

fn parse_query_method(method: &syn::ImplItemFn) -> syn::Result<QueryMethod> {
    let attributes = extract_method_attributes(&method.attrs, "query")?;

    if method.sig.asyncness.is_some() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "#[query] methods must not be async (queries are synchronous)",
        ));
    }

    validate_immut_self_receiver(method)?;

    let input_type = extract_input_type(&method.sig)?;
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

    let input_type = extract_input_type(&method.sig)?;
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

fn extract_input_type(sig: &syn::Signature) -> syn::Result<Option<Type>> {
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
    // Check for WorkflowContext, &WorkflowContext, or &mut WorkflowContext
    let inner_type = match ty {
        Type::Reference(r) => &*r.elem,
        other => other,
    };

    if let Type::Path(TypePath { path, .. }) = inner_type
        && let Some(segment) = path.segments.last()
    {
        return segment.ident == "WorkflowContext";
    }
    false
}

fn extract_workflow_result_type(sig: &syn::Signature) -> syn::Result<Option<Type>> {
    match &sig.output {
        ReturnType::Type(_, ty) => {
            if let Type::Path(TypePath { path, .. }) = &**ty
                && let Some(segment) = path.segments.last()
                && segment.ident == "WorkflowResult"
                && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
                && let Some(syn::GenericArgument::Type(output_ty)) = args.args.first()
            {
                return Ok(Some(output_ty.clone()));
            }
            Ok(None)
        }
        ReturnType::Default => Ok(None),
    }
}

fn extract_simple_output_type(sig: &syn::Signature) -> Option<Type> {
    match &sig.output {
        ReturnType::Type(_, ty) => Some((**ty).clone()),
        ReturnType::Default => None,
    }
}

impl WorkflowMethodsDefinition {
    fn run_struct_ident(&self) -> syn::Ident {
        let struct_name = method_name_to_pascal_case(&self.run_method.method.sig.ident);
        format_ident!("{}", struct_name)
    }

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

        let mut marker_structs = Vec::new();
        let mut const_definitions = Vec::new();
        let mut trait_impls = Vec::new();

        // Run method marker struct and impls
        let run_method = &self.run_method;
        let struct_ident = self.run_struct_ident();

        let input_type = type_to_tokens(
            run_method.input_type.as_ref().or(self
                .init_method
                .as_ref()
                .and_then(|m| m.input_type.as_ref())),
        );

        let output_type = type_to_tokens(run_method.output_type.as_ref());

        let workflow_name = if let Some(ref name_expr) = run_method.attributes.name_override {
            quote! { #name_expr }
        } else {
            quote! { #impl_type_name }
        };

        marker_structs.push(generate_marker_struct(&run_method.method));
        const_definitions.push(generate_const_definition(&run_method.method, &module_ident));

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

            impl ::temporalio_common::WorkflowDefinition for #impl_type {
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

        for signal in &self.signals {
            let (structs, consts, impls) =
                self.generate_signal_code(signal, impl_type, &module_ident);
            marker_structs.push(structs);
            const_definitions.push(consts);
            trait_impls.push(impls);
        }

        for query in &self.queries {
            let (structs, consts, impls) =
                self.generate_query_code(query, impl_type, &module_ident);
            marker_structs.push(structs);
            const_definitions.push(consts);
            trait_impls.push(impls);
        }

        for update in &self.updates {
            let (structs, consts, impls) =
                self.generate_update_code(update, impl_type, &module_ident);
            marker_structs.push(structs);
            const_definitions.push(consts);
            trait_impls.push(impls);
        }

        let workflow_impl = self.generate_workflow_implementation(impl_type, &module_ident);
        let implementer_impl = self.generate_workflow_implementer(impl_type, &module_ident);

        let const_impl = quote! {
            impl #impl_type {
                /// Returns the workflow type name
                pub fn name() -> &'static str {
                    <Self as ::temporalio_common::WorkflowDefinition>::name()
                }

                #(#const_definitions)*
            }
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
        let info = HandlerCodegenInfo::new(
            &signal.method,
            &signal.attributes,
            signal.input_type.as_ref(),
            module_ident,
        );
        let struct_ident = &info.struct_ident;
        let input_type = &info.input_type_tokens;
        let signal_name = &info.handler_name;

        let has_input = signal.input_type.is_some();
        let handle_body = if signal.is_async {
            generate_async_handler_body(&info.prefixed_method, has_input)
        } else {
            generate_sync_handler_body(&info.prefixed_method, has_input, false)
        };

        let run_struct_ident = self.run_struct_ident();
        let trait_impl = quote! {
            impl ::temporalio_common::SignalDefinition for #module_ident::#struct_ident {
                type Workflow = #module_ident::#run_struct_ident;
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
                    ctx: &mut ::temporalio_sdk::WorkflowContext,
                    input: <#module_ident::#struct_ident as ::temporalio_common::SignalDefinition>::Input,
                ) -> ::futures_util::future::BoxFuture<'_, ()> {
                    use ::futures_util::FutureExt;
                    #handle_body
                }
            }
        };

        (info.marker_struct, info.const_def, trait_impl)
    }

    fn generate_query_code(
        &self,
        query: &QueryMethod,
        impl_type: &Type,
        module_ident: &syn::Ident,
    ) -> (TokenStream2, TokenStream2, TokenStream2) {
        let info = HandlerCodegenInfo::new(
            &query.method,
            &query.attributes,
            query.input_type.as_ref(),
            module_ident,
        );
        let struct_ident = &info.struct_ident;
        let prefixed_method = &info.prefixed_method;
        let input_type = &info.input_type_tokens;
        let output_type = type_to_tokens(query.output_type.as_ref());
        let query_name = &info.handler_name;

        let method_call = generate_method_call(prefixed_method, query.input_type.is_some());

        let run_struct_ident = self.run_struct_ident();
        let trait_impl = quote! {
            impl ::temporalio_common::QueryDefinition for #module_ident::#struct_ident {
                type Workflow = #module_ident::#run_struct_ident;
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
                    ctx: &::temporalio_sdk::WorkflowContext,
                    input: <#module_ident::#struct_ident as ::temporalio_common::QueryDefinition>::Input,
                ) -> <#module_ident::#struct_ident as ::temporalio_common::QueryDefinition>::Output {
                    #method_call
                }
            }
        };

        (info.marker_struct, info.const_def, trait_impl)
    }

    fn generate_update_code(
        &self,
        update: &UpdateMethod,
        impl_type: &Type,
        module_ident: &syn::Ident,
    ) -> (TokenStream2, TokenStream2, TokenStream2) {
        let info = HandlerCodegenInfo::new(
            &update.method,
            &update.attributes,
            update.input_type.as_ref(),
            module_ident,
        );
        let struct_ident = &info.struct_ident;
        let input_type = &info.input_type_tokens;
        let output_type = type_to_tokens(update.output_type.as_ref());
        let update_name = &info.handler_name;

        let has_input = update.input_type.is_some();
        let handle_body = if update.is_async {
            generate_async_handler_body(&info.prefixed_method, has_input)
        } else {
            generate_sync_handler_body(&info.prefixed_method, has_input, true)
        };

        let run_struct_ident = self.run_struct_ident();
        let trait_impl = quote! {
            impl ::temporalio_common::UpdateDefinition for #module_ident::#struct_ident {
                type Workflow = #module_ident::#run_struct_ident;
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
                    ctx: &mut ::temporalio_sdk::WorkflowContext,
                    input: <#module_ident::#struct_ident as ::temporalio_common::UpdateDefinition>::Input,
                ) -> ::futures_util::future::BoxFuture<'_, <#module_ident::#struct_ident as ::temporalio_common::UpdateDefinition>::Output> {
                    use ::futures_util::FutureExt;
                    #handle_body
                }
            }
        };

        (info.marker_struct, info.const_def, trait_impl)
    }

    fn generate_workflow_implementation(
        &self,
        impl_type: &Type,
        module_ident: &syn::Ident,
    ) -> TokenStream2 {
        let run_method = &self.run_method;

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
                quote! {
                    #impl_type::#prefixed_init(ctx, input.expect("init takes input"))
                }
            } else {
                quote! {
                    #impl_type::#prefixed_init(ctx)
                }
            }
        } else {
            quote! {
                Self::default()
            }
        };

        let run_call = if run_has_input {
            quote! { self.#prefixed_run(&mut ctx, input.expect("run takes input")).await }
        } else {
            quote! { self.#prefixed_run(&mut ctx).await }
        };

        let run_impl_body = quote! {
            use ::futures_util::FutureExt;
            use ::temporalio_common::data_converters::GenericPayloadConverter;
            let converter = ::temporalio_common::data_converters::PayloadConverter::default();
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
        };

        quote! {
            impl ::temporalio_sdk::workflows::WorkflowImplementation for #impl_type {
                type Run = #module_ident::#run_struct_ident;

                const INIT_TAKES_INPUT: bool = #init_has_input;

                fn init(
                    ctx: &::temporalio_sdk::WorkflowContext,
                    input: ::std::option::Option<<Self::Run as ::temporalio_common::WorkflowDefinition>::Input>,
                ) -> Self {
                    #init_body
                }

                fn run(
                    &mut self,
                    ctx: ::temporalio_sdk::WorkflowContext,
                    input: ::std::option::Option<<Self::Run as ::temporalio_common::WorkflowDefinition>::Input>,
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
        let mut registrations = vec![quote! {
            defs.register_workflow_run::<Self>();
        }];

        // Register signals, queries, and updates using the helper
        registrations.extend(
            self.signals
                .iter()
                .map(|s| generate_handler_registration(&s.method, module_ident, "register_signal")),
        );
        registrations.extend(
            self.queries
                .iter()
                .map(|q| generate_handler_registration(&q.method, module_ident, "register_query")),
        );
        registrations.extend(
            self.updates
                .iter()
                .map(|u| generate_handler_registration(&u.method, module_ident, "register_update")),
        );

        quote! {
            impl ::temporalio_sdk::workflows::WorkflowImplementer for #impl_type {
                fn register_all(defs: &mut ::temporalio_sdk::workflows::WorkflowDefinitions) {
                    #(#registrations)*
                }
            }
        }
    }
}
