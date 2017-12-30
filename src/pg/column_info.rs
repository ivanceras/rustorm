use error::DbError;
use dao;
use dao::TableName;
use dao::ColumnName;
use dao::FromDao;
use entity::EntityManager;
use column::{Column, ColumnStat, ColumnConstraint, Literal, ColumnSpecification, Capacity};
use types::SqlType;
use uuid::Uuid;
use types::ArrayType;
use util;

/// get all the columns of the table
pub fn get_columns(em: &EntityManager, table_name: &TableName) -> Result<Vec<Column>, DbError> {

    /// column name and comment
    #[derive(Debug, FromDao)]
    struct ColumnSimple{
        number: i32,
        name: String,
        comment: Option<String>,
    }

    impl ColumnSimple{
        fn to_column(&self, table_name: &TableName, specification: ColumnSpecification, stat: Option<ColumnStat>) -> Column {
            Column{
                table: table_name.clone(),
                name: ColumnName::from(&self.name),
                comment: self.comment.to_owned(),
                specification: specification,             
                stat
            }
        }

    }
    let sql = r#"SELECT 
                 pg_attribute.attnum AS number, 
                 pg_attribute.attname AS name, 
                 pg_description.description AS comment 
            FROM pg_attribute 
       LEFT JOIN pg_class 
              ON pg_class.oid = pg_attribute.attrelid 
       LEFT JOIN pg_namespace 
              ON pg_namespace.oid = pg_class.relnamespace 
       LEFT JOIN pg_description 
              ON pg_description.objoid = pg_class.oid 
             AND pg_description.objsubid = pg_attribute.attnum 
           WHERE 
                 pg_class.relname = $1 
             AND pg_namespace.nspname = $2 
             AND pg_attribute.attnum > 0 
             AND pg_attribute.attisdropped = false 
        ORDER BY number 
    "#;
    let schema = match table_name.schema {
        Some(ref schema) => schema.to_string(),
        None => "public".to_string()
    };
    let columns_simple: Result<Vec<ColumnSimple>, DbError> = 
        em.execute_sql_with_return(&sql, &[&table_name.name, &schema]);

    match columns_simple{
        Ok(columns_simple) => {
            let mut columns = vec![];
            for column_simple in columns_simple{
                let specification = get_column_specification(em, table_name, &column_simple.name);
                let column_stat = get_column_stat(em, table_name, &column_simple.name)?;
                match specification{
                    Ok(specification) => {
                        let column = column_simple.to_column(table_name, specification, column_stat);
                        columns.push(column);
                    },
                    // early return
                    Err(e) => {return Err(e);},
                }
            }
            Ok(columns)
        },
        Err(e) => Err(e),
    }
}


/// get the contrainst of each of this column
fn get_column_specification(em: &EntityManager, table_name: &TableName, column_name: &String)
    -> Result<ColumnSpecification, DbError> {

    /// null, datatype default value
    #[derive(Debug, FromDao)]
    struct ColumnConstraintSimple{
        not_null: bool,
        data_type: String,
        default: Option<String>,
        is_enum: bool,
        is_array_enum: bool,
        enum_choices: Vec<String>,
        array_enum_choices: Vec<String>,
    }

    impl ColumnConstraintSimple{


        fn to_column_specification(&self) -> ColumnSpecification {
            let (sql_type, capacity) = self.get_sql_type_capacity();
            ColumnSpecification{
                 sql_type: sql_type, 
                 capacity: capacity,
                 constraints: self.to_column_constraints(),
            }
        }

        fn to_column_constraints(&self) -> Vec<ColumnConstraint> {
            let (sql_type, _) = self.get_sql_type_capacity();
            let mut constraints = vec![];
            if self.not_null{
                constraints.push(ColumnConstraint::NotNull);
            }
            if let Some(ref default) = self.default{
                let constraint = if default == "null" {
                    ColumnConstraint::DefaultValue(Literal::Null)
                }
                else if default.starts_with("nextval"){
                    ColumnConstraint::AutoIncrement
                }
                else {
                    let literal =  match sql_type {
                        SqlType::Bool => {
                            let v: bool = default.parse().unwrap();
                            Literal::Bool(v)
                        }
                        SqlType::Int 
                            | SqlType::Smallint 
                            | SqlType::Tinyint 
                            | SqlType::Bigint => {
                                let v: Result<i64,_> = default.parse();
                                match v{
                                    Ok(v) => Literal::Integer(v),
                                    Err(e) => panic!("error parsing to integer: {} error: {}", default, e)
                                }
                            },
                        SqlType::Float
                            | SqlType::Double
                            | SqlType::Real
                            | SqlType::Numeric => {
                                // some defaults have cast type example: (0)::numeric
                                let splinters = util::maybe_trim_parenthesis(default).split("::").collect::<Vec<&str>>();
                                let default_value = util::maybe_trim_parenthesis(splinters[0]);
                                if default_value == "NULL" {
                                    Literal::Null
                                }
                                else{
                                    match util::eval_f64(default_value){
                                        Ok(val) => Literal::Double(val),
                                        Err(e) => panic!("unable to evaluate default value expression: {}, error: {}", default_value, e),
                                    }
                                }

                            }
                        SqlType::Uuid => {
                            if default == "uuid_generate_v4()"{
                               Literal::UuidGenerateV4
                            }
                            else{
                                let v: Result<Uuid,_> = Uuid::parse_str(default);
                                match v{
                                    Ok(v) => Literal::Uuid(v),
                                    Err(e) => panic!("error parsing to uuid: {} error: {}", default, e)
                                }
                            }
                        }
                        SqlType::Timestamp
                            | SqlType::TimestampTz
                            => {
                                if default == "now()" || default == "timezone('utc'::text, now())"
                                {
                                    Literal::CurrentTimestamp
                                }
                                else{
                                    panic!("timestamp other than now is not covered")
                                }
                            }
                        SqlType::Date => {
                            // timestamp converted to text then converted to date 
                            // is equivalent to today()
                            if default == "today()" || default == "now()" || default =="('now'::text)::date" {
                                Literal::CurrentDate
                            }
                            else{
                                panic!("date other than today is not covered in {:?}", self)
                            }
                        }
                        SqlType::Varchar 
                            | SqlType::Char
                            | SqlType::Tinytext
                            | SqlType::Mediumtext
                            | SqlType::Text
                                => Literal::String(default.to_owned()),
                        SqlType::Enum(_name, _choices) => Literal::String(default.to_owned()),
                        _ => panic!("not convered: {:?}", sql_type),
                    };
                    ColumnConstraint::DefaultValue(literal)
                };
                constraints.push(constraint);
                
            }
            constraints
        }

        fn get_sql_type_capacity(&self) -> (SqlType, Option<Capacity>) {
            let data_type: &str = &self.data_type;
            let start = data_type.find('(');
            let end = data_type.find(')');
            let (dtype, capacity) = if let Some(start) = start {
                if let Some(end) = end {
                    let dtype = &data_type[0..start];
                    let range = &data_type[start+1..end];
                    let capacity = if range.contains(","){
                        let splinters = range.split(",").collect::<Vec<&str>>();
                        assert!(splinters.len() == 2, "There should only be 2 parts");
                        let range1:Result<i32,_> = splinters[0].parse();
                        let range2:Result<i32,_>= splinters[1].parse();
                        match range1{
                            Ok(r1) => match range2{
                                Ok(r2) => Some(Capacity::Range(r1,r2)),
                                Err(e) => {
                                    println!("error: {} when parsing range2 for data_type: {:?}", e,  data_type);
                                    None 
                                }
                            }
                            Err(e) => {
                                println!("error: {} when parsing range1 for data_type: {:?}", e,  data_type);
                                None
                            }
                        }

                    }
                    else{
                        let limit:Result<i32,_> = range.parse();
                        match limit{
                            Ok(limit) => Some(Capacity::Limit(limit)),
                            Err(e) => {
                                println!("error: {} when parsing limit for data_type: {:?}", e, data_type);
                                None
                            }
                        }
                    };
                    (dtype, capacity)
                }else{
                    (data_type, None)
                }
            }
            else{
                (data_type, None)
            };

            if self.is_enum{
                println!("enum: {}", data_type);
                let enum_type = SqlType::Enum(data_type.to_owned()
                                              , self.enum_choices.to_owned());
                (enum_type, None)
            }
            else if self.is_array_enum && self.array_enum_choices.len() > 0{
                let array_enum = SqlType::ArrayType(ArrayType::Enum(data_type.to_owned()
                                                                    , self.array_enum_choices.to_owned()));
                (array_enum, None)
            }
            else{
                let sql_type = match dtype{
                    "boolean" => SqlType::Bool,
                    "tinyint" => SqlType::Tinyint,
                    "smallint" | "year" => SqlType::Smallint,
                    "int" | "integer" => SqlType::Int,
                    "int[]" | "integer[]" => SqlType::ArrayType(ArrayType::Int),
                    "bigint" => SqlType::Bigint,
                    "real" => SqlType::Real,
                    "float" => SqlType::Float,
                    "double" | "double precision" => SqlType::Double,
                    "numeric" => SqlType::Numeric,
                    "tinyblob" => SqlType::Tinyblob,
                    "mediumblob" => SqlType::Mediumblob,
                    "blob" => SqlType::Blob,
                    "bytea" => SqlType::Blob,
                    "longblob" => SqlType::Longblob,
                    "varbinary" => SqlType::Varbinary,
                    "char" => SqlType::Char,
                    "varchar" | "character varying" | "character" => SqlType::Varchar,
                    "varchar[]" | "character varying[]" => SqlType::ArrayType(ArrayType::Text),
                    "tinytext" => SqlType::Tinytext,
                    "mediumtext" => SqlType::Mediumtext,
                    "text" => SqlType::Text,
                    "json" => SqlType::Json,
                    "tsvector" => SqlType::TsVector,
                    "text[]" => SqlType::ArrayType(ArrayType::Text),
                    "uuid" => SqlType::Uuid,
                    "date" => SqlType::Date,
                    "timestamp" | "timestamp without time zone" => SqlType::Timestamp,
                    "timestamp with time zone" => SqlType::TimestampTz,
                    "time with time zone" => SqlType::TimeTz,
                    "time without time zone" => SqlType::Time,
                    "inet" => SqlType::IpAddress,
                    "real[]" => SqlType::ArrayType(ArrayType::Float),
                    _ => panic!("not yet handled: {}", dtype), 
                };
                (sql_type, capacity)
            }
        }

    }

    let sql = r#"SELECT DISTINCT 
               pg_attribute.attnotnull AS not_null, 
               pg_catalog.format_type(pg_attribute.atttypid, pg_attribute.atttypmod) AS data_type, 
     CASE WHEN pg_attribute.atthasdef THEN pg_attrdef.adsrc 
           END AS default ,
               pg_type.typtype = 'e'::character AS is_enum,
               pg_type.typcategory = 'A'::character AS is_array_enum,
               ARRAY(SELECT enumlabel FROM pg_enum
                        WHERE pg_enum.enumtypid = pg_attribute.atttypid)
               AS enum_choices,
               ARRAY(SELECT enumlabel FROM pg_enum
                        WHERE pg_enum.enumtypid = pg_type.typelem)
               AS array_enum_choices
          FROM pg_attribute 
          JOIN pg_class 
            ON pg_class.oid = pg_attribute.attrelid 
          JOIN pg_type 
            ON pg_type.oid = pg_attribute.atttypid 
     LEFT JOIN pg_attrdef 
            ON pg_attrdef.adrelid = pg_class.oid 
           AND pg_attrdef.adnum = pg_attribute.attnum 
     LEFT JOIN pg_namespace 
            ON pg_namespace.oid = pg_class.relnamespace 
     LEFT JOIN pg_constraint 
            ON pg_constraint.conrelid = pg_class.oid 
           AND pg_attribute.attnum = ANY (pg_constraint.conkey) 
         WHERE  
               pg_attribute.attname = $1 
           AND pg_class.relname = $2 
           AND pg_namespace.nspname = $3 
           AND pg_attribute.attisdropped = false 
    "#;
    let schema = match table_name.schema {
        Some(ref schema) => schema.to_string(),
        None => "public".to_string()
    };
    //println!("sql: {} column_name: {}, table_name: {}", sql, column_name, table_name.name);
    let column_constraint: Result<ColumnConstraintSimple, DbError> = 
        em.execute_sql_with_one_return(&sql, &[&column_name, &table_name.name, &schema]);
    column_constraint
        .map(|c| c.to_column_specification() )
}

fn get_column_stat(em: &EntityManager, table_name: &TableName, column_name: &String)
    -> Result<Option<ColumnStat>, DbError> {
        let sql = r#"
            SELECT avg_width,
                n_distinct
            FROM pg_stats
           WHERE 
                pg_stats.schemaname = $3
            AND pg_stats.tablename = $2
            AND pg_stats.attname = $1
        "#;
        let schema = match table_name.schema {
            Some(ref schema) => schema.to_string(),
            None => "public".to_string()
        };
        let column_stat: Result<Option<ColumnStat>, DbError>
            = em.execute_sql_with_maybe_one_return(&sql, &[column_name, &table_name.name, &schema]);
        column_stat
}




#[cfg(test)]
mod test{

    use super::*;
    use pool::Pool;
    use dao::ToDao;
    use dao::ToColumnNames;
    use dao::ToTableName;
    use chrono::offset::Utc;
    use chrono::DateTime;


    #[test]
    fn insert_text_array(){
        #[derive(Debug, ToDao, ToColumnNames, ToTableName)]
        struct Film{
            title: String, 
            language_id: i16,
            special_features: Vec<String>, 
        }

        #[derive(Debug, FromDao, ToColumnNames)]
        struct RetrieveFilm{
            film_id: i32,
            title: String, 
            language_id: i16,
            special_features: Vec<String>, 
            last_update: DateTime<Utc>,
        }

        let film1 = Film{
            title: "Hurry potter and the prisoner is escaing".into(),
            language_id: 1,
            special_features: vec!["fantasy".into(), "magic".into()],
        };
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let result: Result<Vec<RetrieveFilm>,DbError> = em.insert(&[&film1]);
        println!("result: {:#?}",result);
        assert!(result.is_ok());
    }

    #[test]
    fn column_specification_for_film_rating(){
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let table = TableName::from("film");
        let column = ColumnName::from("rating");
        let specification = get_column_specification(&em, &table, &column.name);
        println!("specification: {:#?}", specification);
        assert!(specification.is_ok());
        let specification = specification.unwrap();
        assert_eq!(specification, ColumnSpecification{
                           sql_type: SqlType::Enum("mpaa_rating".into(), vec!["G".into(), "PG".into(), "PG-13".into(), "R".into(), "NC-17".into()]),
                           capacity: None,
                           constraints: vec![ColumnConstraint::DefaultValue(Literal::String("'G'::mpaa_rating".into()))],
                       });
    }

    #[test]
    fn column_specification_for_actor_id(){
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let actor_table = TableName::from("actor");
        let actor_id_column = ColumnName::from("actor_id");
        let specification = get_column_specification(&em, &actor_table, &actor_id_column.name);
        println!("specification: {:#?}", specification);
        assert!(specification.is_ok());
        let specification = specification.unwrap();
        assert_eq!(specification, ColumnSpecification{
                           sql_type: SqlType::Int,
                           capacity: None,
                           constraints: vec![ColumnConstraint::NotNull,
                           ColumnConstraint::AutoIncrement],
                       });

    }
    #[test]
    fn column_specification_for_actor_last_updated(){
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let actor_table = TableName::from("actor");
        let column = ColumnName::from("last_update");
        let specification = get_column_specification(&em, &actor_table, &column.name);
        println!("specification: {:#?}", specification);
        assert!(specification.is_ok());
        let specification = specification.unwrap();
        assert_eq!(specification, ColumnSpecification{
                           sql_type: SqlType::Timestamp,
                           capacity: None,
                           constraints: vec![ColumnConstraint::NotNull,
                           ColumnConstraint::DefaultValue(Literal::CurrentTimestamp)],
                       });
    }

    #[test]
    fn column_for_actor(){
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let actor_table = TableName::from("actor");
        let columns = get_columns(&em, &actor_table);
        println!("columns: {:#?}", columns);
        assert!(columns.is_ok());
        let columns = columns.unwrap();
        assert_eq!(columns.len(), 4);
        assert_eq!(columns[1].name, ColumnName{
                                    name: "first_name".to_string(),
                                    table: None, 
                                    alias: None
        });
        assert_eq!(columns[1].specification, 
                       ColumnSpecification{
                           sql_type: SqlType::Varchar,
                           capacity: Some(Capacity::Limit(45)),
                           constraints: vec![ColumnConstraint::NotNull],
                       }
               );
    }

    #[test]
    fn column_for_film(){
        let db_url = "postgres://postgres:p0stgr3s@localhost:5432/sakila";
        let mut pool = Pool::new();
        let em = pool.em(db_url);
        assert!(em.is_ok());
        let em = em.unwrap();
        let table = TableName::from("film");
        let columns = get_columns(&em, &table);
        println!("columns: {:#?}", columns);
        assert!(columns.is_ok());
        let columns = columns.unwrap();
        assert_eq!(columns.len(), 14);
        assert_eq!(columns[7].name, ColumnName::from("rental_rate"));
        assert_eq!(columns[7].specification, 
                       ColumnSpecification{
                           sql_type: SqlType::Numeric,
                           capacity: Some(Capacity::Range(4,2)),
                           constraints: vec![ColumnConstraint::NotNull,
                                    ColumnConstraint::DefaultValue(Literal::Double(4.99))
                                ],
                       }
                 );
    }

}
