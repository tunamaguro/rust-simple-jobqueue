use std::marker::PhantomData;
use std::sync::Arc;

use futures::{StreamExt as _, TryStreamExt as _};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sqlx::PgPool;

#[derive(Debug, Clone)]
struct Client<Data = ()> {
    phantom: PhantomData<Data>,
    pool: sqlx::PgPool,
}

impl Client<()> {
    const fn new<Data>(pool: sqlx::PgPool) -> Client<Data> {
        Client::<Data> {
            phantom: PhantomData,
            pool,
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
    fn enqueue(&self, data: Data) -> impl Future<Output = Result<(), ClientError>> {
        self.enqueue_tx(data, &self.pool)
    }

    async fn enqueue_tx<'a, A>(&self, data: Data, tx: A) -> Result<(), ClientError>
    where
        A: sqlx::Acquire<'a, Database = sqlx::Postgres>,
    {
        let mut conn = tx.acquire().await?;
        let args = serde_json::to_value(data)?;
        sqlx::query!(
            "
            INSERT INTO job_waiting (args) VALUES ($1)
            ",
            args
        )
        .execute(&mut *conn)
        .await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct JobData<T> {
    id: sqlx::types::Uuid,
    data: T,
}

#[derive(Debug, Clone)]
struct Poller {
    pool: sqlx::PgPool,
}

impl Poller {
    fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    async fn poll_and_run<F, Fut, T, E>(
        &self,
        func: F,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>
    where
        F: Fn(T) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        T: serde::de::DeserializeOwned,
        E: std::error::Error + Send + Sync + 'static,
    {
        let mut tx = self.pool.begin().await?;

        struct JobData {
            id: sqlx::types::Uuid,
            args: serde_json::Value,
        }

        let data = sqlx::query_as!(
            JobData,
            r#"
            DELETE 
            FROM job_waiting
            WHERE id = (
                SELECT 
                    id
                FROM job_waiting 
                ORDER BY id 
                LIMIT 1 
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id,args
        "#
        )
        .fetch_one(&mut *tx)
        .await?;

        let args = serde_json::from_value::<T>(data.args)?;
        func(args).await?;
        tx.commit().await?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Message {
    text: String,
}

#[tokio::main]
async fn main() {
    let pool = PgPool::connect("postgres://root:password@postgres:5432/app")
        .await
        .unwrap();

    let client = Client::new::<Message>(pool.clone());
    let poller = Arc::new(Poller::new(pool));

    let mut st = futures::stream::iter(0..100)
        .map(|num| {
            let client = client.clone();

            tokio::spawn(async move {
                let client = client;
                println!("Insert message {num}!");
                let fut = client.enqueue(Message {
                    text: format!("Message {num}!"),
                });
                fut.await
            })
        })
        .buffer_unordered(8)
        .map(|x| x.unwrap())
        .map(|_| {
            let poller = poller.clone();
            tokio::spawn(async move {
                let poller = poller;
                let fut = poller.poll_and_run(async |data: Message| {
                    println!("message: {}", data.text);
                    Ok::<(), std::io::Error>(())
                });
                fut.await
            })
        })
        .buffer_unordered(8);

    while (st.next().await).is_some() {}
}
