use futures::{FutureExt as _, Stream, StreamExt as _};
use serde::{Serialize, de::DeserializeOwned};
use tracing::Instrument;

#[allow(warnings)]
mod queries;

pub struct Job<Context, Data> {
    context: Context,
    pub data: Data,
}

const PG_LEASE_SECONDS: i32 = 60;

#[derive(Debug)]
pub struct PgOutTxContext {
    id: sqlx::types::Uuid,
    pool: sqlx::PgPool,
}

impl PgOutTxContext {
    async fn heartbeat(&self) -> Result<(), anyhow::Error> {
        queries::HeartbeatJob::builder()
            .id(self.id)
            .lease_seconds(PG_LEASE_SECONDS)
            .build()
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn complete(&self) -> Result<(), anyhow::Error> {
        queries::CompleteJob::builder()
            .id(self.id)
            .build()
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn fail(&self) -> Result<(), anyhow::Error> {
        queries::FailJob::builder()
            .id(self.id)
            .build()
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn retry(&self, retry_after: Option<std::time::Duration>) -> Result<(), anyhow::Error> {
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

pub type PgJob<T> = Job<PgOutTxContext, T>;

struct Ticker {
    inner: tokio::time::Interval,
}

impl Ticker {
    fn new(period: std::time::Duration) -> Self {
        let start = tokio::time::Instant::now() + period;
        Self {
            inner: tokio::time::interval_at(start, period),
        }
    }

    fn tick_after(self) -> Self {
        let period = self.inner.period();
        let start = tokio::time::Instant::now() + period;
        Self {
            inner: tokio::time::interval_at(start, period),
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

    pub async fn poll_job(&self, batch_size: u16) -> Vec<Result<PgJob<T>, anyhow::Error>> {
        let q = queries::PollJobs::builder()
            .batch_size(batch_size.into())
            .lease_seconds(PG_LEASE_SECONDS)
            .build();
        let jobs = q
            .query_as()
            .fetch(&self.pool)
            .map(|res| match res {
                Ok(row) => {
                    let data = serde_json::from_value::<T>(row.args)?;
                    Ok(PgJob {
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
        tracing::trace!("Fetched {} jobs", jobs.len());
        jobs
    }

    pub fn to_stream(
        &self,
        interval: std::time::Duration,
        batch_size: u16,
    ) -> impl Stream<Item = Result<PgJob<T>, anyhow::Error>> + 'static {
        self.to_stream_until(interval, batch_size, futures::future::pending::<()>())
    }

    pub fn to_stream_until<Fut>(
        &self,
        interval: std::time::Duration,
        batch_size: u16,
        signal: Fut,
    ) -> impl Stream<Item = Result<PgJob<T>, anyhow::Error>> + 'static
    where
        Fut: Future + Send + 'static,
    {
        let ticker = Ticker::new(interval).take_until(signal);
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
    T: DeserializeOwned,
    S: Stream<Item = Result<PgJob<T>, anyhow::Error>>,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = JobResult> + Send,
{
    pub fn new(stream: S, concurrent: usize, handler: F) -> Self {
        Self {
            data_type: std::marker::PhantomData,
            stream,
            concurrent,
            handler,
        }
    }

    pub async fn run(self) {
        let stream = self.stream;

        let fut = stream
            .filter_map(|job| async {
                job.inspect_err(|error| tracing::error!(error = %error, "Failed to fetch job"))
                    .ok()
            })
            .map(|job| async {
                let context = job.context;
                let data = job.data;

                tracing::trace!("Start handler");
                let result = {
                    let hb_every = 1.max(PG_LEASE_SECONDS / 3) as u64;
                    let mut ticker = Ticker::new(std::time::Duration::from_secs(hb_every)).tick_after();

                    let mut tick = ticker.next().fuse();
                    let handler_span = tracing::debug_span!("Job handler");
                    let mut handler_fut = (self.handler)(data).instrument(handler_span).boxed().fuse();
                    loop {
                        futures::select! {
                            res = handler_fut => break res,
                            _ = tick =>{
                                let span = tracing::debug_span!("Job heartbeat");
                                let _res = context.heartbeat().instrument(span).await.inspect_err(
                                    |error| tracing::error!(error = %error, "Failed to heartbeat job"),
                                );
                                tick = ticker.next().fuse();
                            }
                        }
                    }
                };
                tracing::trace!("Finish handler");
                match result {
                    JobResult::Success => {
                        let span = tracing::debug_span!("Complete job");
                        let _res = context.complete().instrument(span).await.inspect_err(
                            |error| tracing::error!(error = %error, "Failed to complete job"),
                        );
                    }
                    JobResult::Retry(retry_after) => {
                        let span = tracing::debug_span!("Retry job");
                        let _res = context.retry(retry_after).instrument(span).await.inspect_err(
                            |error| tracing::error!(error = %error, "Failed to retry job"),
                        );
                    }
                    JobResult::Abort => {
                        let span = tracing::debug_span!("Abort job");
                        let _res = context.fail().instrument(span).await.inspect_err(
                            |error| tracing::error!(error = %error, "Failed to abort job"),
                        );
                    }
                };
            })
            .buffer_unordered(self.concurrent)
            .for_each(async |_| {});

        fut.await;
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
