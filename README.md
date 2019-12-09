[![Build Status](https://travis-ci.org/ivanceras/rustorm.svg?branch=master)](https://travis-ci.org/ivanceras/rustorm)

# rustorm


### Rustorm

[![Financial Contributors on Open Collective](https://opencollective.com/rustorm/all/badge.svg?label=financial+contributors)](https://opencollective.com/rustorm) [![Latest Version](https://img.shields.io/crates/v/rustorm.svg)](https://crates.io/crates/rustorm)
[![Build Status](https://travis-ci.org/ivanceras/rustorm.svg?branch=master)](https://travis-ci.org/ivanceras/rustorm)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

Rustorm is an SQL-centered ORM with focus on ease of use on conversion of database types to
their appropriate rust type.

Selecting records

```rust
use rustorm::{
    DbError,
    FromDao,
    Pool,
    ToColumnNames,
    ToTableName,
};

#[derive(Debug, FromDao, ToColumnNames, ToTableName)]
struct Actor {
    actor_id: i32,
    first_name: String,
}

#[cfg(any(feature="with-postgres", feature = "with-sqlite"))]
fn main() {
    let mut pool = Pool::new();
    #[cfg(feature = "with-sqlite")]
    let db_url = "sqlite://sakila.db";
    #[cfg(feature = "with-postgres")]
    let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
    let em = pool.em(db_url).unwrap();
    let sql = "SELECT * FROM actor LIMIT 10";
    let actors: Result<Vec<Actor>, DbError> =
        em.execute_sql_with_return(sql, &[]);
    println!("Actor: {:#?}", actors);
    let actors = actors.unwrap();
    assert_eq!(actors.len(), 10);
    for actor in actors {
        println!("actor: {:?}", actor);
    }
}
#[cfg(feature="with-mysql")]
fn main() {
   println!("see examples for mysql usage, mysql has a little difference in the api");
}
```
Inserting and displaying the inserted records

```rust
use chrono::{
    offset::Utc,
    DateTime,
    NaiveDate,
};
use rustorm::{
    DbError,
    FromDao,
    Pool,
    TableName,
    ToColumnNames,
    ToDao,
    ToTableName,
};


#[cfg(any(feature="with-postgres", feature = "with-sqlite"))]
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

    let mut pool = Pool::new();
    #[cfg(feature = "with-sqlite")]
    let db_url = "sqlite://sakila.db";
    #[cfg(feature = "with-postgres")]
    let db_url = "postgres://postgres:p0stgr3s@localhost/sakila";
    let em = pool.em(db_url).unwrap();
    let tom_cruise = for_insert::Actor {
        first_name: "TOM".into(),
        last_name: "CRUISE".to_string(),
    };
    let tom_hanks = for_insert::Actor {
        first_name: "TOM".into(),
        last_name: "HANKS".to_string(),
    };

    let actors: Result<Vec<for_retrieve::Actor>, DbError> =
        em.insert(&[&tom_cruise, &tom_hanks]);
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
#[cfg(feature="with-mysql")]
fn main() {
   println!("see examples for mysql usage, mysql has a little difference in the api");
}
```
Rustorm is wholly used by [diwata](https://github.com/ivanceras/diwata)

License: MIT

## Contributors

### Code Contributors

This project exists thanks to all the people who contribute. [[Contribute](CONTRIBUTING.md)].
<a href="https://github.com/ivanceras/rustorm/graphs/contributors"><img src="https://opencollective.com/rustorm/contributors.svg?width=890&button=false" /></a>

### Financial Contributors

Become a financial contributor and help us sustain our community. [[Contribute](https://opencollective.com/rustorm/contribute)]

#### Individuals

<a href="https://opencollective.com/rustorm"><img src="https://opencollective.com/rustorm/individuals.svg?width=890"></a>

#### Organizations

Support this project with your organization. Your logo will show up here with a link to your website. [[Contribute](https://opencollective.com/rustorm/contribute)]

<a href="https://opencollective.com/rustorm/organization/0/website"><img src="https://opencollective.com/rustorm/organization/0/avatar.svg"></a>
<a href="https://opencollective.com/rustorm/organization/1/website"><img src="https://opencollective.com/rustorm/organization/1/avatar.svg"></a>
<a href="https://opencollective.com/rustorm/organization/2/website"><img src="https://opencollective.com/rustorm/organization/2/avatar.svg"></a>
<a href="https://opencollective.com/rustorm/organization/3/website"><img src="https://opencollective.com/rustorm/organization/3/avatar.svg"></a>
<a href="https://opencollective.com/rustorm/organization/4/website"><img src="https://opencollective.com/rustorm/organization/4/avatar.svg"></a>
<a href="https://opencollective.com/rustorm/organization/5/website"><img src="https://opencollective.com/rustorm/organization/5/avatar.svg"></a>
<a href="https://opencollective.com/rustorm/organization/6/website"><img src="https://opencollective.com/rustorm/organization/6/avatar.svg"></a>
<a href="https://opencollective.com/rustorm/organization/7/website"><img src="https://opencollective.com/rustorm/organization/7/avatar.svg"></a>
<a href="https://opencollective.com/rustorm/organization/8/website"><img src="https://opencollective.com/rustorm/organization/8/avatar.svg"></a>
<a href="https://opencollective.com/rustorm/organization/9/website"><img src="https://opencollective.com/rustorm/organization/9/avatar.svg"></a>
