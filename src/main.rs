use futures::StreamExt;
use simple_jobqueue::{JobResult, PgBackEnd, PgClient, Worker};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct Message {
    n: u32,
    text: String,
}

#[tokio::main]
async fn main() {
    let pool = sqlx::PgPool::connect("postgres://root:password@postgres:5432/app")
        .await
        .unwrap();

    let backend = PgBackEnd::<Message>::new(pool.clone());
    let data_stream = backend.to_stream(std::time::Duration::from_millis(1000), 8);
    let worker = Worker::new(data_stream.boxed(), 8, job_handler);

    let worker_handle = tokio::spawn(worker.run());

    let client = PgClient::<Message>::new(pool.clone());
    let client_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(200));
        let mut n = 0;
        loop {
            interval.tick().await;
            let message = Message {
                n,
                text: format!("Message {}", n),
            };
            client.enqueue(message).await?;
            println!("Enqueued job {}", n);
            n += 1;
            if n > 500 {
                break;
            }
        }

        Result::<(), anyhow::Error>::Ok(())
    });

    let _ = client_handle.await;
    worker_handle.await.unwrap().unwrap();
}

async fn job_handler(job: Message) -> JobResult {
    let handle = tokio::spawn(async move {
        println!("-start: job {}", job.n);
        tokio::time::sleep(std::time::Duration::from_millis(
            ((job.n + 1) % 5 * 500).into(),
        ))
        .await;

        println!("--end: job {}", job.n);
    });

    match handle.await {
        Ok(_) => JobResult::Success,
        Err(_) => JobResult::Retry(None),
    }
}
