use crate::error::HealthCheckError;
use reqwest::Client;
use std::time::Duration;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

pub async fn healthcheck(
    endpoint: String,
    shutdown: CancellationToken,
) -> Result<(), HealthCheckError> {
    let client = Client::builder()
        .user_agent(format!(
            "microcosm slingshot v{} (dev: @bad-example.com)",
            env!("CARGO_PKG_VERSION")
        ))
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()?;

    loop {
        tokio::select! {
            res = client.get(&endpoint).send() => {
                let _ = res
                    .and_then(|r| r.error_for_status())
                    .inspect_err(|e| log::error!("failed to send healthcheck: {e}"));
            },
            _ = shutdown.cancelled() => break,
        }
        sleep(Duration::from_secs(51)).await;
    }
    Ok(())
}
