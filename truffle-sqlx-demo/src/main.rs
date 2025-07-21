use std::str::FromStr;

use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqlitePool;

#[tokio::main]
async fn main() {
    let opts = SqliteConnectOptions::from_str("./test.db")
        .unwrap()
        .create_if_missing(true);
    let db = SqlitePool::connect_with(opts).await.unwrap();

    sqlx::migrate!().run(&db).await.unwrap();

    truffle_sqlx::query!(
        r#"
        insert into account
        values (?, ?, ?, ?);
        "#
    )
    .bind(0)
    .bind("John Doe")
    .bind("johndoe@example.com")
    .bind("password")
    .execute(&db)
    .await
    .unwrap();

    truffle_sqlx::query!("select * from account where id = ?")
        .bind(0)
        .execute(&db)
        .await
        .unwrap();

    truffle_sqlx::query!(
        r#"
        insert into item
        (id, name)
        values (?, ?)      
    "#
    )
    .bind(0)
    .bind("New Item")
    .execute(&db)
    .await
    .unwrap();
}
