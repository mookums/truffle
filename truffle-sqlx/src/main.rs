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

    truffle_macro::query!("select * from user where id = ?")
        .bind("abc")
        .execute(&db)
        .await
        .unwrap();

    let list: Vec<i32> = truffle_macro::query_scalar!("select * from user limit 1")
        .fetch_all(&db)
        .await
        .unwrap();

    assert!(list.is_empty())
}
