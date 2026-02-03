# truffle - an SQL static analyzer

```toml
[dependencies]
truffle = { git = "https://github.com/mookums/truffle", features = [ "all" ] }
truffle-sqlx = { git = "https://github.com/mookums/truffle", features = [ "all" ] }
```

Truffle is a comprehensive SQL static analyzer designed to catch errors, validate syntax, and ensure query correctness at compile time. With Truffle, you can eliminate common SQL-related runtime errors and improve the reliability of your database interactions.

We check a variety of things, including but not limited to: SQL syntax, column resolution, alias conflicting, scoping (row vs group), type checking, etc.

Truffle is available as a Rust library (`truffle`), a CLI (`truffle-cli`), through C FFI (planned), and as an `sqlx` wrapper. You can also try out a WASM demo on my website [here](https://muki.gg/demo/truffle).

## Installation & Usage
### Rust Crate 
Add Truffle to your `Cargo.toml`.
```toml
truffle = { git = "https://github.com/mookums/truffle" }
```

For specific features, feel free to enable various flags (or use the `all` feature to get everything.)
```toml
truffle = { git = "https://github.com/mookums/truffle", features = [ "all" ] }
```

You can now use Truffle within your project, like the example below.
```rust
use truffle::Simulator;

fn main() {
    let mut sim = Simulator::default();
    sim.execute("create table person (id int primary key, name text not null)").unwrap();
    sim.execute("select * from person").unwrap();
}
```
### CLI
You can install the CLI using `cargo`
```bash
cargo install --git https://github.com/mookums/truffle truffle-cli
```

For more information with the CLI, feel free to run:
```bash
truffle --help
```

### With `sqlx`
The `truffle-sqlx` provides experimental but functional integration with `sqlx`, allowing for your queries to be checked at compile-time without the use of a database.

Add the `truffle-sqlx` crate to your `Cargo.toml`.
```toml
truffle-sqlx = { git = "https://github.com/mookums/truffle" }
```

Then you can now use truffle-sqlx in place of the standard sqlx macros. These will give you errors in place when your SQL does not match the expected schema, utilizing your `migrations` folder.
```rust
use sqlx::sqlite::SqlitePool;

#[derive(Debug)]
pub struct Account {
    pub id: i32,
    pub name: String,
    pub email: String,
}

#[tokio::main]
async fn main() {
    let db = SqlitePool::connect(":memory:").await.unwrap();
    sqlx::migrate!().run(&db).await.unwrap();

    truffle_sqlx::query!(
        r#"
        insert into account
        values ($1, $2, $3);
        "#,
        0,
        "John Doe",
        None::<String>,
    )
    .execute(&db)
    .await
    .unwrap();

    let account = truffle_sqlx::query_as!(Account, "select * from account where id = ?", 0)
        .fetch_one(&db)
        .await
        .unwrap();

    let email: String = truffle_sqlx::query_as!("select email from account where id = ?", 0)
        .fetch_one(&db)
        .await
        .map(|p| p.email.unwrap_or_else(|| "No Email".to_string()))
        .unwrap();

    println!("Fetched Item: {account:?}");
    println!("Email: {email:?}");
}
```

## Contributing
We welcome contributions to Truffle! Please see our contributing guidelines and code of conduct in the repository.
Areas where we need help:

- Additional database dialect support
- Documentation improvements
- Test coverage expansion (especially with SQL edge-cases)
