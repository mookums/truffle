use std::str::FromStr;

use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqlitePool;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AccountStatus {
    Active = 1,
    Inactive = 2,
    Deleted = 3,
}

impl From<AccountStatus> for i32 {
    fn from(value: AccountStatus) -> Self {
        match value {
            AccountStatus::Active => 1,
            AccountStatus::Inactive => 2,
            AccountStatus::Deleted => 3,
        }
    }
}

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
        values (?, ?, ?, ?, ?);
        "#,
        0,
        "John Doe",
        "johndoe@example.com",
        "password1",
        AccountStatus::Active,
    )
    .execute(&db)
    .await
    .unwrap();

    let item = truffle_sqlx::query_as!(
        r#"
        select * from account
        where id = ?
        "#,
        0
    )
    .fetch_one(&db)
    .await
    .unwrap();

    println!("Fetched Item: {item:?}");
}
