pub mod consumer;
pub mod server;
pub mod subscriber;

use serde::Serialize;

#[derive(Debug, Clone)]
pub struct LinkEvent {
    collection: String,
    path: String,
    origin: String,
    target: String,
    rev: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all="snake_case")]
pub struct ClientEvent {
    kind: String,
    link: ClientLinkEvent,
}

#[derive(Debug, Serialize)]
struct ClientLinkEvent {
    operation: String,
    source: String,
    source_record: String,
    source_rev: String,
    subject: String,
    // TODO: include the record too? would save clients a level of hydration
}

impl From<LinkEvent> for ClientLinkEvent {
    fn from(link: LinkEvent) -> Self {
        let undotted = link.path.strip_prefix('.').unwrap_or_else(|| {
            eprintln!("link path did not have expected '.' prefix: {}", link.path);
            ""
        });
        Self {
            operation: "create".to_string(),
            source: format!("{}:{undotted}", link.collection),
            source_record: link.origin,
            source_rev: link.rev,
            subject: link.target,
        }
    }
}
