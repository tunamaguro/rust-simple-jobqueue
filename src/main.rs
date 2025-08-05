use std::marker::PhantomData;

use serde::{Serialize, de::DeserializeOwned};
use sqlx::PgPool;

struct Client<Data = ()> {
    phantom: PhantomData<Data>,
}

impl Client<()> {
    const fn new<Data>() -> Client<Data> {
        Client::<Data> {
            phantom: PhantomData,
        }
    }
}

#[derive(Debug)]
enum ClientError {
    DataBase(sqlx::Error),
    Encode(serde_json::Error),
}

impl From<sqlx::Error> for ClientError {
    fn from(value: sqlx::Error) -> Self {
        ClientError::DataBase(value)
    }
}

impl From<serde_json::Error> for ClientError {
    fn from(value: serde_json::Error) -> Self {
        ClientError::Encode(value)
    }
}

impl<Data: Serialize + DeserializeOwned> Client<Data> {
    async fn enqueue<'a, A>(&self, data: Data, conn: A) -> Result<(), ClientError>
    where
        A: sqlx::Acquire<'a, Database = sqlx::Postgres>,
    {
        let mut conn = conn.acquire().await?;
        let args = serde_json::to_value(data)?;
        sqlx::query!(
            "
            INSERT INTO job (args) VALUES ($1)
        ",
            args
        )
        .execute(&mut *conn)
        .await
        .unwrap();
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let pool = PgPool::connect("postgres://root:password@postgres:5432/app")
        .await
        .unwrap();

    let client = Client::new::<String>();
    client.enqueue("12345".to_string(), &pool).await.unwrap();

    println!("Hello, world!");
}
