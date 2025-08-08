use sqlx::sqlite::SqlitePool;

#[derive(Debug)]
pub struct Account {
    pub id: i32,
    pub name: String,
    pub email: String,
    pub password: String,
    pub status: AccountStatus,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AccountStatus {
    Active = 1,
    Inactive = 2,
    Deleted = 3,
}

impl From<i32> for AccountStatus {
    fn from(value: i32) -> Self {
        match value {
            1 => AccountStatus::Active,
            2 => AccountStatus::Inactive,
            3 => AccountStatus::Deleted,
            _ => unreachable!(),
        }
    }
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
    let db = SqlitePool::connect(":memory:").await.unwrap();
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

    let account = truffle_sqlx::query_as!(
        Account,
        r#"
        select * from account
        where id = ?
        "#,
        0
    )
    .fetch_one(&db)
    .await
    .unwrap();

    let id_name: (String, AccountStatus) =
        truffle_sqlx::query_as!("select name, status from account where id = ?", 0)
            .fetch_one(&db)
            .await
            .map(|p| (p.name, p.status.into()))
            .unwrap();

    println!("Fetched Item: {account:?}");
    println!("Item Pair: {id_name:?}");
}
