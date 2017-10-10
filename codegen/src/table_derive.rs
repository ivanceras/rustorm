
use syn;
use quote;


pub fn impl_to_table_name(ast: &syn::MacroInput) -> quote::Tokens {
    let name = &ast.ident;
    quote! {
        impl ToTableName for  #name {

            fn to_table_name() -> table_name::TableName {
                table_name::TableName{
                    name: stringify!(#name).to_lowercase().into(),
                    schema: None,
                    alias: None,
                }
            }
        }
    }
}
