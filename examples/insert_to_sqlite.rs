use chrono::{NaiveDateTime, NaiveDate, NaiveTime};
use rustorm::{DbError, FromDao, Pool, ToColumnNames, ToDao, ToTableName, Value};

fn main() {
    mod for_insert {
        use super::*;
        #[derive(Debug, PartialEq, ToDao, ToColumnNames, ToTableName)]
        pub struct Actor {
            pub first_name: String,
            pub last_name: String,
            pub somedate: NaiveDateTime,
        }
    }

    mod for_retrieve {
        use super::*;
        #[derive(Debug, FromDao, ToColumnNames, ToTableName)]
        pub struct Actor {
            pub actor_id: i64,
            pub first_name: String,
            pub last_name: String,
            pub somedate: NaiveDateTime,
            pub last_update: NaiveDateTime,
        }
    }
    let create_sql = "CREATE TABLE actor(
                actor_id integer PRIMARY KEY AUTOINCREMENT,
                first_name text,
                last_name text,
                somedate text,
                last_update timestamp DEFAULT current_timestamp
        )";

    let db_url = "sqlite:///tmp/sqlite.db";
    let mut pool = Pool::new();
    let mut em = pool.em(db_url).unwrap();
    let ret = em.db().execute_sql_with_return(create_sql, &[]);
    println!("ret: {:?}", ret);
    assert!(ret.is_ok());

    let d = NaiveDate::from_ymd(2000, 10, 9);
    let t = NaiveTime::from_hms_milli(15, 2, 55, 2);

    let tom_cruise = for_insert::Actor {
        first_name: "TOM".into(),
        last_name: "CRUISE".to_string(),
        somedate: NaiveDateTime::new(d,t),
    };

    let d = NaiveDate::from_ymd(2000, 10, 9);
    let t = NaiveTime::from_hms_milli(15, 2, 55, 22);
    let tom_hanks = for_insert::Actor {
        first_name: "TOM".into(),
        last_name: "HANKS".to_string(),
        somedate: NaiveDateTime::new(d,t),
    };

    let d = NaiveDate::from_ymd(2000, 10, 9);
    let t = NaiveTime::from_hms_milli(15, 2, 55, 222);
    let tom_selleck = for_insert::Actor {
        first_name: "TOM".into(),
        last_name: "SELLECK".to_string(),
        somedate: NaiveDateTime::new(d,t),
    };
    println!("tom_cruise: {:#?}", tom_cruise);
    println!("tom_hanks: {:#?}", tom_hanks);
    println!("tom_selleck: {:#?}", tom_selleck);

    let actors = vec![tom_cruise, tom_hanks, tom_selleck];

    for actor in actors {
        let first_name: Value = actor.first_name.into();
        let last_name: Value = actor.last_name.into();
        let somedate: Value = actor.somedate.into();
        let ret = em.db().execute_sql_with_return(
            "INSERT INTO actor(first_name, last_name, somedate)
            VALUES ($1, $2, $3)",
            &[&first_name, &last_name, &somedate],
        );
        assert!(ret.is_ok());
    }

    let actors: Result<Vec<for_retrieve::Actor>, DbError> =
        em.execute_sql_with_return("SELECT * from actor", &[]);
    println!("Actor: {:#?}", actors);
    assert!(actors.is_ok());
}
