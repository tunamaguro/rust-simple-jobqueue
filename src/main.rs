use std::marker::PhantomData;
use std::sync::Arc;

use futures::{FutureExt, StreamExt as _, TryStreamExt as _};
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

struct Ticker {
    inner: tokio::time::Interval,
}

impl Ticker {
    fn new(interval: std::time::Duration) -> Self {
        Self {
            inner: tokio::time::interval(interval),
        }
    }
}

impl futures::Stream for Ticker {
    type Item = ();

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_tick(cx).map(|_| Some(()))
    }
}

#[derive(Debug)]
pub struct JobData<T> {
    context: PollerContext,
    data: T,
}

#[derive(Debug)]
pub struct PollerContext {
    id: sqlx::types::Uuid,
    tx: sqlx::Transaction<'static, sqlx::Postgres>,
}

#[derive(Debug, Clone)]
struct Poller {
    pool: sqlx::PgPool,
}

impl Poller {
    fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    async fn poll<T: serde::de::DeserializeOwned>(
        &self,
    ) -> Result<JobData<T>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let mut tx = self.pool.begin().await?;

        let data = sqlx::query!(
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

        let context = PollerContext { id: data.id, tx };
        let value = serde_json::from_value::<T>(data.args)?;

        let data = JobData {
            context,
            data: value,
        };

        Ok(data)
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
        let job = self.poll::<T>().await?;
        func(job.data).await?;
        job.context.tx.commit().await?;

        Ok(())
    }
}

pub struct Runner {
    ticker: Ticker,
    poller: Poller,
}

impl Runner {
    fn new(poller: Poller, interval: std::time::Duration) -> Self {
        Self {
            ticker: Ticker::new(interval),
            poller,
        }
    }

    pub async fn run<F, Fut, T, E>(
        self,
        func: F,
        concurrent: usize,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>
    where
        F: Fn(T) -> Fut + Clone + Send + Sync + 'static,
        Fut: Future<Output = Result<(), E>>,
        T: serde::de::DeserializeOwned,
        E: std::error::Error + Send + Sync + 'static,
    {
        let data_stream = self
            .ticker
            .then(|_| self.poller.poll::<T>())
            .filter_map(|res| async move { res.ok() })
            .map(|job| async {
                let func = func.clone();
                let fut = func(job.data);
                if let Err(e) = fut.await {
                    eprintln!("Error processing job: {}", e);
                }
                job.context.tx.commit().await
            })
            .buffer_unordered(concurrent);
        let mut data_stream = Box::pin(data_stream);

        while let Some(job) = data_stream.next().await {
            job?;
        }
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

    let st = futures::stream::iter(0..100)
        .map(move |num| {
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
        .buffer_unordered(8);

    let push_handle = tokio::spawn(st.for_each_concurrent(1, |_v| async {}));

    let poller = Poller::new(pool);
    let runner = Runner::new(poller, std::time::Duration::from_millis(100));
    let run_handle = runner.run(
        |data: Message| async move {
            let _res = tokio::spawn(async move {
                println!("Processing: {}", data.text);
            })
            .await;

            Result::<(), std::convert::Infallible>::Ok(())
        },
        16,
    );

    let _ = tokio::join!(push_handle, run_handle);
}
