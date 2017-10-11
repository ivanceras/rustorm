
use syn;
use quote;


pub fn impl_to_table_name(ast: &syn::MacroInput) -> quote::Tokens {
    let name = &ast.ident;
    quote! {
        impl ToTableName for  #name {

            fn to_table_name() -> dao::TableName {
                dao::TableName{
                    name: stringify!(#name).to_lowercase().into(),
                    schema: None,
                    alias: None,
                }
            }
        }
    }
}
