pub mod consumer;
pub mod server;

use serde::Serialize;

#[derive(Debug, Clone)]
pub struct LinkEvent {
    collection: String,
    path: String,
    origin: String,
    target: String,
}

#[derive(Debug, Serialize)]
struct ClientEvent {
    source: String,
    origin: String,
    target: String,
    // TODO: include the record too? would save clients a level of hydration
}

impl From<LinkEvent> for ClientEvent {
    fn from(link: LinkEvent) -> Self {
        let undotted = link.path.get(1..).unwrap_or("");
        Self {
            source: format!("{}:{undotted}", link.collection),
            origin: link.origin,
            target: link.target,
        }
    }
}
