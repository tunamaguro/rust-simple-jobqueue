use simple_jobqueue::{JobResult, PgBackEnd, PgClient, Worker};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct Message {
    n: u32,
    text: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let pool = sqlx::PgPool::connect("postgres://root:password@postgres:5432/app")
        .await
        .unwrap();

    let backend = PgBackEnd::<Message>::new(pool.clone());

    let data_stream = backend.to_stream(std::time::Duration::from_millis(1000), 8);
    let worker = Worker::new(data_stream, 8, job_handler);

    let worker_handle = tokio::spawn(worker.run());

    let client = PgClient::<Message>::new(pool.clone());
    // let client_handle = tokio::spawn(async move {
    //     let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
    //     let mut n = 0;
    //     loop {
    //         interval.tick().await;
    //         let message = Message {
    //             n,
    //             text: format!("Message {}", n),
    //         };
    //         client.enqueue(message).await?;
    //         tracing::info!("Enqueued job {}", n);
    //         n += 1;
    //     }
    //     #[allow(unreachable_code)]
    //     Result::<(), anyhow::Error>::Ok(())
    // });

    // let _ = client_handle.await.unwrap();
    worker_handle.await.unwrap();
}

async fn job_handler(job: Message) -> JobResult {
    let handle = tokio::spawn(async move {
        tracing::info!("-start: job {}", job.n);
        tokio::time::sleep(std::time::Duration::from_secs(
            ((job.n + 1) % 5 * 10).into(),
        ))
        .await;

        tracing::info!("--end: job {}", job.n);
    });

    match handle.await {
        Ok(_) => JobResult::Success,
        Err(_) => JobResult::Retry(None),
    }
}
