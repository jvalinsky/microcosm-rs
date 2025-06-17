use crate::{ClientEvent, LinkEvent};
use dropshot::{
    ApiDescription, ConfigDropshot, ConfigLogging, ConfigLoggingLevel, Query, RequestContext,
    ServerBuilder, WebsocketConnection, channel,
};
use futures::SinkExt;
use schemars::JsonSchema;
use serde::Deserialize;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::Role;

pub async fn serve(b: broadcast::Sender<LinkEvent>) -> Result<(), String> {
    let config_logging = ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Info,
    };

    let log = config_logging
        .to_logger("example-basic")
        .map_err(|error| format!("failed to create logger: {}", error))?;

    let mut api = ApiDescription::new();
    api.register(subscribe).unwrap();

    let server = ServerBuilder::new(api, b, log)
        .config(ConfigDropshot {
            bind_address: "0.0.0.0:9998".parse().unwrap(),
            ..Default::default()
        })
        .start()
        .map_err(|error| format!("failed to create server: {}", error))?;

    server.await
}

#[derive(Deserialize, JsonSchema)]
struct QueryParams {
    _hello: Option<String>,
}

#[channel {
    protocol = WEBSOCKETS,
    path = "/subscribe",
}]
async fn subscribe(
    ctx: RequestContext<broadcast::Sender<LinkEvent>>,
    _qp: Query<QueryParams>,
    upgraded: WebsocketConnection,
) -> dropshot::WebsocketChannelResult {
    let mut ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
        upgraded.into_inner(),
        Role::Server,
        None,
    )
    .await;
    let mut sub = ctx.context().subscribe();

    // TODO: pingpong
    // TODO: filtering subscription

    loop {
        match sub.recv().await {
            Ok(link) => {
                let json = serde_json::to_string::<ClientEvent>(&link.into())?;
                if let Err(e) = ws.send(Message::Text(json.into())).await {
                    eprintln!("client: failed to send event: {e:?}");
                    ws.close(None).await?; // TODO: do we need this one??
                    break;
                }
            }
            Err(broadcast::error::RecvError::Closed) => {
                ws.close(None).await?; // TODO: send reason
                break;
            }
            Err(broadcast::error::RecvError::Lagged(_n_missed)) => {
                eprintln!("client lagged, closing");
                ws.close(None).await?; // TODO: send reason
                break;
            }
        }
    }
    Ok(())
}
