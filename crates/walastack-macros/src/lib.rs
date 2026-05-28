//! # walastack-macros
//!
//! Procedural macros for ergonomic WalaStack development.
//!
//! Provides:
//! - `#[walastack::main]` — wraps an async `main` function in a Tokio
//!   multi-threaded runtime plus tracing initialization.
//! - `#[get("/")]`, `#[post("/")]`, `#[put("/")]`, `#[delete("/")]` — route
//!   attribute macros that register the decorated handler on an [`App`].
//!
//! The macros generate code that references `::walastack` and
//! `::walastack::__macro_support::*`, so the user only needs `walastack` in
//! their dependencies.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{ItemFn, LitStr, parse_macro_input};

/// Wrap an async `main` function in a Tokio runtime plus tracing setup.
///
/// # Example
///
/// ```ignore
/// #[walastack::main]
/// async fn main() -> walastack::Result<()> {
///     // ...
/// }
/// ```
#[proc_macro_attribute]
pub fn main(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    if input.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            input.sig.fn_token,
            "#[walastack::main] requires an async fn",
        )
        .to_compile_error()
        .into();
    }

    let attrs = &input.attrs;
    let vis = &input.vis;
    let sig = &input.sig;
    let block = &input.block;

    let fn_name = &sig.ident;
    let return_type = &sig.output;

    let expanded = quote! {
        #(#attrs)*
        #vis fn #fn_name() #return_type {
            ::walastack::init_tracing();
            let runtime = ::walastack::__macro_support::tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("walastack: failed to build runtime");
            runtime.block_on(async move #block)
        }
    };

    expanded.into()
}

/// Register the decorated async function as a `GET` handler at the given path.
///
/// # Example
///
/// ```ignore
/// #[get("/")]
/// async fn index() -> &'static str { "hello" }
///
/// let app = walastack::App::new().route(index);
/// ```
#[proc_macro_attribute]
pub fn get(attr: TokenStream, item: TokenStream) -> TokenStream {
    route_attribute(attr, item, &quote!(get))
}

/// Register the decorated async function as a `POST` handler at the given path.
#[proc_macro_attribute]
pub fn post(attr: TokenStream, item: TokenStream) -> TokenStream {
    route_attribute(attr, item, &quote!(post))
}

/// Register the decorated async function as a `PUT` handler at the given path.
#[proc_macro_attribute]
pub fn put(attr: TokenStream, item: TokenStream) -> TokenStream {
    route_attribute(attr, item, &quote!(put))
}

/// Register the decorated async function as a `DELETE` handler at the given path.
#[proc_macro_attribute]
pub fn delete(attr: TokenStream, item: TokenStream) -> TokenStream {
    route_attribute(attr, item, &quote!(delete))
}

fn route_attribute(
    attr: TokenStream,
    item: TokenStream,
    method_ident: &TokenStream2,
) -> TokenStream {
    let path = parse_macro_input!(attr as LitStr);
    let input_fn = parse_macro_input!(item as ItemFn);

    if input_fn.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            input_fn.sig.fn_token,
            "walastack route handlers must be async fn",
        )
        .to_compile_error()
        .into();
    }

    let fn_name = &input_fn.sig.ident;

    let expanded = quote! {
        #[allow(non_camel_case_types, missing_docs, clippy::module_name_repetitions)]
        pub struct #fn_name;

        impl ::walastack::Route for #fn_name {
            fn register(self, app: ::walastack::App) -> ::walastack::App {
                #input_fn
                app.#method_ident(#path, #fn_name)
            }
        }
    };

    expanded.into()
}
