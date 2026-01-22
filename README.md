# LuhORM

A compile-time ORM that generates type-safe database code through build-time introspection.
Supporting Sqlite and Postgresql out the box but extensible to another database

## Features

- **Compile-time code generation** - Zero runtime reflection, all code generated at build time
- **Type-safe queries** - Builder pattern with compile-time checked columns
- **Foreign key relationships** - Automatic join methods and aggregation helpers
- **Multiple databases** - Built-in support for SQLite and PostgreSQL
- **Migration system** - Hash-verified migrations with automatic tracking

## Quick Start

### Prerequisites

- must have rand & chrono in Cargo.toml
- must have sqlx


1. Add to `build.rs`:
```rust
use luhorm::Codegen;
use sqlx::sqlite::SqlitePoolOptions;

#[tokio::main]
async fn main() {
    let pool = SqlitePoolOptions::new()
        .connect("sqlite:my_db.db")
        .await
        .unwrap();
    
    Codegen::new("orm", pool, "migrations", None)
        .await
        .unwrap()
        .run_codegen()
        .unwrap();
}
```

2. Include generated code in `src/main.rs`:
```rust
mod orm {
    include!(concat!(env!("OUT_DIR"), "/orm.rs"));
}
```

3. Use it:
```rust
use crate::orm::{users::{Users, UsersBuilder}, entry::AggregateEntryUsers};

let users = Users::query()
    .name("alice")
    .age_gt(18)
    .fetch_all(&pool)
    .await?;

let x = Users::query()
    // could do this aswell
    // .join(Entry::NAME, Users::ID.of(), Entry::USERID.of())
    .join_entry()
    .fetch_with_entry(&pool)
    .await?
    .one_to_many()?;

// new feature might change
let new_user = UsersBuilder::new()
    .populate_fake_data()
    .build();

new_user.insert(&pool).await?;
```

## Current Limitations

- Transaction support would be tricky or at least idk I'm still thinking about how I want to do it
- Composite keys are a myth

> made with love - s.c 2026
