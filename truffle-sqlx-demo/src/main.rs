use sqlx::sqlite::SqlitePool;
use truffle_sqlx::{
    convert::{FromSql, IntoSql},
    dialect::Dialect,
};

#[derive(Debug)]
pub struct Account {
    pub id: i32,
    pub name: String,
    pub email: Option<String>,
    pub password: String,
    pub status: AccountStatus,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AccountStatus {
    Active = 1,
    Inactive = 2,
    Deleted = 3,
}

impl<D: Dialect> IntoSql<i32, D> for AccountStatus {
    fn into_sql_type(self) -> i32 {
        self as i32
    }
}

impl<D: Dialect> FromSql<i32, D> for AccountStatus {
    fn from_sql_type(value: i32) -> Self {
        match value {
            1 => AccountStatus::Active,
            2 => AccountStatus::Inactive,
            3 => AccountStatus::Deleted,
            _ => unreachable!(),
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
        values ($1, $2, $3, $4, $5);
        "#,
        0,
        "John Doe",
        None::<String>,
        "password1",
        AccountStatus::Active,
    )
    .execute(&db)
    .await
    .unwrap();

    let account = truffle_sqlx::query_as!(Account, "select * from account where id = ?", 0)
        .fetch_one(&db)
        .await
        .unwrap();

    let name_status: (String, i32) =
        truffle_sqlx::query_as!("select name, status from account where id = ?", 0)
            .fetch_one(&db)
            .await
            .map(|p| (p.name, p.status))
            .unwrap();

    let email: String = truffle_sqlx::query_as!("select email from account where id = ?", 0)
        .fetch_one(&db)
        .await
        .map(|p| p.email.unwrap_or_else(|| "No Email".to_string()))
        .unwrap();

    println!("Fetched Item: {account:?}");
    println!("Item Pair: {name_status:?}");
    println!("Email: {email:?}");
}
