#![deny(warnings)]

extern crate proc_macro;
#[macro_use]
extern crate quote;
extern crate rustorm_dao;
extern crate syn;

#[macro_use]
mod column_derive;
#[macro_use]
mod dao_derive;
#[macro_use]
mod table_derive;

use proc_macro::TokenStream;

#[proc_macro_derive(FromDao)]
pub fn from_dao(input: TokenStream) -> TokenStream {
    let s = input.to_string();
    let ast = syn::parse_macro_input(&s).unwrap();
    let gen = dao_derive::impl_from_dao(&ast);
    gen.parse().unwrap()
}

#[proc_macro_derive(ToDao)]
pub fn to_dao(input: TokenStream) -> TokenStream {
    let s = input.to_string();
    let ast = syn::parse_macro_input(&s).unwrap();
    let gen = dao_derive::impl_to_dao(&ast);
    gen.parse().unwrap()
}

#[proc_macro_derive(ToTableName)]
pub fn to_table_name(input: TokenStream) -> TokenStream {
    let s = input.to_string();
    let ast = syn::parse_macro_input(&s).unwrap();
    let gen = table_derive::impl_to_table_name(&ast);
    gen.parse().unwrap()
}

#[proc_macro_derive(ToColumnNames)]
pub fn to_column_names(input: TokenStream) -> TokenStream {
    let s = input.to_string();
    let ast = syn::parse_macro_input(&s).unwrap();
    let gen = column_derive::impl_to_column_names(&ast);
    gen.parse().unwrap()
}
