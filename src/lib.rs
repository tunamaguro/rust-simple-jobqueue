use anyhow::Context;
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use serde::{Serialize, de::DeserializeOwned};

#[allow(warnings)]
mod queries;

pub struct Job<Context, Data> {
    context: Context,
    pub data: Data,
}

#[derive(Debug)]
pub struct PgOutTxContext {
    id: sqlx::types::Uuid,
    pool: sqlx::PgPool,
}

impl PgOutTxContext {
    async fn complete(self) -> Result<(), anyhow::Error> {
        queries::CompleteJob::builder()
            .id(self.id)
            .build()
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn fail(self) -> Result<(), anyhow::Error> {
        queries::FailJob::builder()
            .id(self.id)
            .build()
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn retry(self, retry_after: Option<std::time::Duration>) -> Result<(), anyhow::Error> {
        let duration = retry_after.unwrap_or(std::time::Duration::from_secs(15));
        let interval = sqlx::postgres::types::PgInterval::try_from(duration)
            .map_err(|e| anyhow::anyhow!(e))?;

        queries::RetryJob::builder()
            .id(self.id)
            .interval(&interval)
            .build()
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

pub type PgJobData<T> = Job<PgOutTxContext, T>;

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

pub struct PgBackEnd<T> {
    pool: sqlx::PgPool,
    job_data: std::marker::PhantomData<T>,
}

impl<T> Clone for PgBackEnd<T> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            job_data: std::marker::PhantomData,
        }
    }
}

impl<T> PgBackEnd<T>
where
    T: DeserializeOwned + Send + 'static,
{
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            job_data: std::marker::PhantomData,
        }
    }

    pub async fn poll_job(&self, batch_size: usize) -> Vec<Result<PgJobData<T>, anyhow::Error>> {
        let q = queries::PollJobs::builder()
            .limit(batch_size.try_into().unwrap_or(5))
            .build();
        let jobs = q
            .query_as()
            .fetch(&self.pool)
            .map(|res| match res {
                Ok(row) => {
                    let data = serde_json::from_value::<T>(row.args)?;
                    Ok(PgJobData {
                        context: PgOutTxContext {
                            id: row.id,
                            pool: self.pool.clone(),
                        },
                        data,
                    })
                }
                Err(err) => Err(anyhow::anyhow!(err)),
            })
            .collect::<Vec<_>>()
            .await;

        jobs
    }

    pub fn to_stream(
        &self,
        interval: std::time::Duration,
        batch_size: usize,
    ) -> impl Stream<Item = Result<PgJobData<T>, anyhow::Error>> + 'static {
        let ticker = Ticker::new(interval);
        let backend = std::sync::Arc::new(self.clone());
        let jobs_st = ticker.then(move |_| {
            let backend = backend.clone();
            async move { backend.poll_job(batch_size).await }
        });
        let jobs_st = jobs_st.flat_map(futures::stream::iter);

        jobs_st
    }
}

pub enum JobResult {
    Success,
    Retry(Option<std::time::Duration>),
    Abort,
}

pub struct Worker<T, S, F> {
    data_type: std::marker::PhantomData<T>,
    stream: S,
    concurrent: usize,
    handler: F,
}

impl<T, S, F, Fut> Worker<T, S, F>
where
    T: DeserializeOwned + Send + 'static,
    S: Stream<Item = Result<PgJobData<T>, anyhow::Error>> + Unpin,
    F: Fn(T) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = JobResult> + Send + 'static,
{
    pub fn new(stream: S, concurrent: usize, handler: F) -> Self {
        Self {
            data_type: std::marker::PhantomData,
            stream,
            concurrent,
            handler,
        }
    }

    pub async fn run(self) -> Result<(), anyhow::Error> {
        let stream = self.stream;

        let fut = stream
            .map(|job| async {
                let job = match job {
                    Ok(job) => job,
                    Err(e) => {
                        println!("Error fetching job: {:?}", e);
                        return Err(e);
                    }
                };

                let context = job.context;
                let data = job.data;
                println!("Running handler");
                let result = (self.handler)(data).await;
                println!("Finish handler");
                match result {
                    JobResult::Success => {
                        context.complete().await.context("failed to complete job")?
                    }
                    JobResult::Retry(retry_after) => context
                        .retry(retry_after)
                        .await
                        .context("failed to retry job")?,
                    JobResult::Abort => context.fail().await.context("failed to abort job")?,
                };
                Result::<(), anyhow::Error>::Ok(())
            })
            .buffer_unordered(self.concurrent)
            .try_fold((), |_, res| async move { Ok(res) });

        fut.await?;

        Ok(())
    }
}

pub struct PgClient<T> {
    data_type: std::marker::PhantomData<T>,
    pool: sqlx::PgPool,
}

impl<T> Clone for PgClient<T> {
    fn clone(&self) -> Self {
        Self {
            data_type: std::marker::PhantomData,
            pool: self.pool.clone(),
        }
    }
}

impl<T> PgClient<T>
where
    T: Serialize,
{
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            data_type: std::marker::PhantomData,
            pool,
        }
    }

    pub fn enqueue(&self, data: T) -> impl Future<Output = Result<(), anyhow::Error>> {
        Self::enqueue_tx(data, &self.pool)
    }

    pub async fn enqueue_tx<'a, A>(data: T, tx: A) -> Result<(), anyhow::Error>
    where
        A: sqlx::Acquire<'a, Database = sqlx::Postgres>,
    {
        let mut conn = tx.acquire().await?;
        let args = serde_json::to_value(data)?;
        queries::InsertJob::builder()
            .job_data(&args)
            .build()
            .execute(&mut *conn)
            .await?;

        Ok(())
    }
}
