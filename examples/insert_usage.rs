use chrono::{
    offset::Utc,
    DateTime,
};
use rustorm::{
    DbError,
    FromDao,
    Pool,
    ToColumnNames,
    ToDao,
    ToTableName,
};

fn main() {
    mod for_insert {
        use super::*;
        #[derive(Debug, PartialEq, ToDao, ToColumnNames, ToTableName)]
        pub struct Actor {
            pub first_name: String,
            pub last_name: String,
        }
    }

    mod for_retrieve {
        use super::*;
        #[derive(Debug, FromDao, ToColumnNames, ToTableName)]
        pub struct Actor {
            pub actor_id: i32,
            pub first_name: String,
            pub last_name: String,
            pub last_update: DateTime<Utc>,
        }
    }

    let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
    let mut pool = Pool::new();
    let mut em = pool.em(db_url).unwrap();
    let tom_cruise = for_insert::Actor {
        first_name: "TOM".into(),
        last_name: "CRUISE".to_string(),
    };
    let tom_hanks = for_insert::Actor {
        first_name: "TOM".into(),
        last_name: "HANKS".to_string(),
    };
    println!("tom_cruise: {:#?}", tom_cruise);
    println!("tom_hanks: {:#?}", tom_hanks);

    let actors: Result<Vec<for_retrieve::Actor>, DbError> = em.insert(&[&tom_cruise, &tom_hanks]);
    println!("Actor: {:#?}", actors);
    assert!(actors.is_ok());
    let actors = actors.unwrap();
    let today = Utc::now().date();
    assert_eq!(tom_cruise.first_name, actors[0].first_name);
    assert_eq!(tom_cruise.last_name, actors[0].last_name);
    assert_eq!(today, actors[0].last_update.date());
    assert_eq!(tom_hanks.first_name, actors[1].first_name);
    assert_eq!(tom_hanks.last_name, actors[1].last_name);
    assert_eq!(today, actors[1].last_update.date());
}
