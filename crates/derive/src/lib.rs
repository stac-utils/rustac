use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

#[proc_macro_derive(SelfHref)]
pub fn self_href_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let expanded = quote! {
        impl ::stac::SelfHref for #name {
            fn self_href(&self) -> Option<&str> {
                self.self_href.as_deref()
            }
            fn self_href_mut(&mut self) -> &mut Option<String> {
                &mut self.self_href
            }
        }
    };
    TokenStream::from(expanded)
}

#[proc_macro_derive(Links)]
pub fn links_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let expanded = quote! {
        impl ::stac::Links for #name {
            fn links(&self) -> &[::stac::Link] {
                &self.links
            }
            fn links_mut(&mut self) -> &mut Vec<::stac::Link> {
                &mut self.links
            }
        }
    };
    TokenStream::from(expanded)
}

#[proc_macro_derive(Migrate)]
pub fn migrate_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let expanded = quote! {
        impl ::stac::Migrate for #name {}
    };
    TokenStream::from(expanded)
}

#[proc_macro_derive(Fields)]
pub fn fields_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let expanded = quote! {
        impl ::stac::Fields for #name {
            fn fields(&self) -> &serde_json::Map<String, serde_json::Value> {
                &self.additional_fields
            }
            fn fields_mut(&mut self) -> &mut serde_json::Map<String, Value> {
                &mut self.additional_fields
            }
        }
    };
    TokenStream::from(expanded)
}
