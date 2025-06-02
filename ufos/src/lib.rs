pub mod consumer;
pub mod db_types;
pub mod error;
pub mod file_consumer;
pub mod index_html;
pub mod qs_query;
pub mod server;
pub mod storage;
pub mod storage_fjall;
pub mod storage_mem;
pub mod store_types;

use crate::error::BatchInsertError;
use crate::store_types::SketchSecretPrefix;
use cardinality_estimator_safe::{Element, Sketch};
use error::FirehoseEventError;
use jetstream::events::{CommitEvent, CommitOp, Cursor};
use jetstream::exports::{Did, Nsid, RecordKey};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::value::RawValue;
use sha2::Sha256;
use std::collections::HashMap;
use std::time::Duration;

fn did_element(sketch_secret: &SketchSecretPrefix, did: &Did) -> Element<14> {
    Element::from_digest_with_prefix::<Sha256>(sketch_secret, did.as_bytes())
}

pub fn nice_duration(dt: Duration) -> String {
    let secs = dt.as_secs_f64();
    if secs < 1. {
        return format!("{:.0}ms", secs * 1000.);
    }
    if secs < 60. {
        return format!("{secs:.02}s");
    }
    let mins = (secs / 60.).floor();
    let rsecs = secs - (mins * 60.);
    if mins < 60. {
        return format!("{mins:.0}m{rsecs:.0}s");
    }
    let hrs = (mins / 60.).floor();
    let rmins = mins - (hrs * 60.);
    if hrs < 24. {
        return format!("{hrs:.0}h{rmins:.0}m{rsecs:.0}s");
    }
    let days = (hrs / 24.).floor();
    let rhrs = hrs - (days * 24.);
    format!("{days:.0}d{rhrs:.0}h{rmins:.0}m{rsecs:.0}s")
}

#[derive(Debug, Default, Clone)]
pub struct CollectionCommits<const LIMIT: usize> {
    pub creates: usize,
    pub updates: usize,
    pub deletes: usize,
    pub dids_estimate: Sketch<14>,
    pub commits: Vec<UFOsCommit>,
    head: usize,
}

impl<const LIMIT: usize> CollectionCommits<LIMIT> {
    fn advance_head(&mut self) {
        self.head += 1;
        if self.head > LIMIT {
            self.head = 0;
        }
    }
    /// lossy-ish commit insertion
    ///
    /// - new commits are *always* added to the batch or else rejected as full.
    /// - when LIMIT is reached, new commits can displace existing `creates`.
    ///   `update`s and `delete`s are *never* displaced.
    /// - if all batched `creates` have been displaced, the batch is full.
    ///
    /// in general it's rare for commits to be displaced except for very high-
    /// volume collections such as `app.bsky.feed.like`.
    ///
    /// it could be nice in the future to retain all batched commits and just
    /// drop new `creates` after a limit instead.
    pub fn truncating_insert(
        &mut self,
        commit: UFOsCommit,
        sketch_secret: &SketchSecretPrefix,
    ) -> Result<(), BatchInsertError> {
        if (self.updates + self.deletes) == LIMIT {
            // nothing can be displaced (only `create`s may be displaced)
            return Err(BatchInsertError::BatchFull(commit));
        }

        // every kind of commit counts as "user activity"
        self.dids_estimate
            .insert(did_element(sketch_secret, &commit.did));

        match commit.action {
            CommitAction::Put(PutAction {
                is_update: false, ..
            }) => {
                self.creates += 1;
            }
            CommitAction::Put(PutAction {
                is_update: true, ..
            }) => {
                self.updates += 1;
            }
            CommitAction::Cut => {
                self.deletes += 1;
            }
        }

        if self.commits.len() < LIMIT {
            // normal insert: there's space left to put a new commit at the end
            self.commits.push(commit);
        } else {
            // displacement insert: find an old `create` we can displace
            let head_started_at = self.head;
            loop {
                let candidate = self
                    .commits
                    .get_mut(self.head)
                    .ok_or(BatchInsertError::BatchOverflow(self.head))?;
                if candidate.action.is_create() {
                    *candidate = commit;
                    break;
                }
                self.advance_head();
                if self.head == head_started_at {
                    return Err(BatchInsertError::BatchForever);
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DeleteAccount {
    pub did: Did,
    pub cursor: Cursor,
}

#[derive(Debug, Clone)]
pub enum CommitAction {
    Put(PutAction),
    Cut,
}
impl CommitAction {
    pub fn is_create(&self) -> bool {
        match self {
            CommitAction::Put(PutAction { is_update, .. }) => !is_update,
            CommitAction::Cut => false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PutAction {
    record: Box<RawValue>,
    is_update: bool,
}

#[derive(Debug, Clone)]
pub struct UFOsCommit {
    cursor: Cursor,
    did: Did,
    rkey: RecordKey,
    rev: String,
    action: CommitAction,
}

#[derive(Debug, Clone, Serialize)]
pub struct UFOsRecord {
    pub cursor: Cursor,
    pub did: Did,
    pub collection: Nsid,
    pub rkey: RecordKey,
    pub rev: String,
    // TODO: cid?
    pub record: Box<RawValue>,
    pub is_update: bool,
}

impl UFOsCommit {
    pub fn from_commit_info(
        commit: CommitEvent,
        did: Did,
        cursor: Cursor,
    ) -> Result<(Self, Nsid), FirehoseEventError> {
        let action = match commit.operation {
            CommitOp::Delete => CommitAction::Cut,
            cru => CommitAction::Put(PutAction {
                record: commit.record.ok_or(FirehoseEventError::CruMissingRecord)?,
                is_update: cru == CommitOp::Update,
            }),
        };
        let batched = Self {
            cursor,
            did,
            rkey: commit.rkey,
            rev: commit.rev,
            action,
        };
        Ok((batched, commit.collection))
    }
}

#[derive(Debug, Default, Clone)]
pub struct EventBatch<const LIMIT: usize> {
    pub commits_by_nsid: HashMap<Nsid, CollectionCommits<LIMIT>>,
    pub account_removes: Vec<DeleteAccount>,
}

impl<const LIMIT: usize> EventBatch<LIMIT> {
    pub fn insert_commit_by_nsid(
        &mut self,
        collection: &Nsid,
        commit: UFOsCommit,
        max_collections: usize,
        sketch_secret: &SketchSecretPrefix,
    ) -> Result<(), BatchInsertError> {
        let map = &mut self.commits_by_nsid;
        if !map.contains_key(collection) && map.len() >= max_collections {
            return Err(BatchInsertError::BatchFull(commit));
        }
        map.entry(collection.clone())
            .or_default()
            .truncating_insert(commit, sketch_secret)?;
        Ok(())
    }
    pub fn total_collections(&self) -> usize {
        self.commits_by_nsid.len()
    }
    pub fn account_removes(&self) -> usize {
        self.account_removes.len()
    }
    pub fn estimate_dids(&self) -> usize {
        let mut estimator = Sketch::<14>::default();
        for commits in self.commits_by_nsid.values() {
            estimator.merge(&commits.dids_estimate);
        }
        estimator.estimate()
    }
    pub fn latest_cursor(&self) -> Option<Cursor> {
        let mut oldest = Cursor::from_start();
        for commits in self.commits_by_nsid.values() {
            for commit in &commits.commits {
                if commit.cursor > oldest {
                    oldest = commit.cursor;
                }
            }
        }
        if let Some(del) = self.account_removes.last() {
            if del.cursor > oldest {
                oldest = del.cursor;
            }
        }
        if oldest > Cursor::from_start() {
            Some(oldest)
        } else {
            None
        }
    }
    pub fn is_empty(&self) -> bool {
        self.commits_by_nsid.is_empty() && self.account_removes.is_empty()
    }
}

#[derive(Debug, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum ConsumerInfo {
    Jetstream {
        endpoint: String,
        started_at: u64,
        latest_cursor: Option<u64>,
        rollup_cursor: Option<u64>,
    },
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct NsidCount {
    nsid: String,
    creates: u64,
    dids_estimate: u64,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct JustCount {
    creates: u64,
    dids_estimate: u64,
}

#[derive(Debug)]
pub enum OrderCollectionsBy {
    Lexi { cursor: Option<Vec<u8>> },
    RecordsCreated,
    DidsEstimate,
}
impl Default for OrderCollectionsBy {
    fn default() -> Self {
        Self::Lexi { cursor: None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncating_insert_truncates() -> anyhow::Result<()> {
        let mut commits: CollectionCommits<2> = Default::default();

        commits.truncating_insert(
            UFOsCommit {
                cursor: Cursor::from_raw_u64(100),
                did: Did::new("did:plc:whatever".to_string()).unwrap(),
                rkey: RecordKey::new("rkey-asdf-a".to_string()).unwrap(),
                rev: "rev-asdf".to_string(),
                action: CommitAction::Put(PutAction {
                    record: RawValue::from_string("{}".to_string())?,
                    is_update: false,
                }),
            },
            &[0u8; 16],
        )?;

        commits.truncating_insert(
            UFOsCommit {
                cursor: Cursor::from_raw_u64(101),
                did: Did::new("did:plc:whatever".to_string()).unwrap(),
                rkey: RecordKey::new("rkey-asdf-b".to_string()).unwrap(),
                rev: "rev-asdg".to_string(),
                action: CommitAction::Put(PutAction {
                    record: RawValue::from_string("{}".to_string())?,
                    is_update: false,
                }),
            },
            &[0u8; 16],
        )?;

        commits.truncating_insert(
            UFOsCommit {
                cursor: Cursor::from_raw_u64(102),
                did: Did::new("did:plc:whatever".to_string()).unwrap(),
                rkey: RecordKey::new("rkey-asdf-c".to_string()).unwrap(),
                rev: "rev-asdh".to_string(),
                action: CommitAction::Put(PutAction {
                    record: RawValue::from_string("{}".to_string())?,
                    is_update: false,
                }),
            },
            &[0u8; 16],
        )?;

        assert_eq!(commits.creates, 3);
        assert_eq!(commits.dids_estimate.estimate(), 1);
        assert_eq!(commits.commits.len(), 2);

        let mut found_first = false;
        let mut found_last = false;
        for commit in commits.commits {
            match commit.rev.as_ref() {
                "rev-asdf" => {
                    found_first = true;
                }
                "rev-asdh" => {
                    found_last = true;
                }
                _ => {}
            }
        }
        assert!(!found_first);
        assert!(found_last);

        Ok(())
    }

    #[test]
    fn test_truncating_insert_counts_updates() -> anyhow::Result<()> {
        let mut commits: CollectionCommits<2> = Default::default();

        commits.truncating_insert(
            UFOsCommit {
                cursor: Cursor::from_raw_u64(100),
                did: Did::new("did:plc:whatever".to_string()).unwrap(),
                rkey: RecordKey::new("rkey-asdf-a".to_string()).unwrap(),
                rev: "rev-asdf".to_string(),
                action: CommitAction::Put(PutAction {
                    record: RawValue::from_string("{}".to_string())?,
                    is_update: true,
                }),
            },
            &[0u8; 16],
        )?;

        assert_eq!(commits.creates, 0);
        assert_eq!(commits.updates, 1);
        assert_eq!(commits.deletes, 0);
        assert_eq!(commits.dids_estimate.estimate(), 1);
        assert_eq!(commits.commits.len(), 1);
        Ok(())
    }

    #[test]
    fn test_truncating_insert_does_not_truncate_deletes() -> anyhow::Result<()> {
        let mut commits: CollectionCommits<2> = Default::default();

        commits.truncating_insert(
            UFOsCommit {
                cursor: Cursor::from_raw_u64(100),
                did: Did::new("did:plc:whatever".to_string()).unwrap(),
                rkey: RecordKey::new("rkey-asdf-a".to_string()).unwrap(),
                rev: "rev-asdf".to_string(),
                action: CommitAction::Cut,
            },
            &[0u8; 16],
        )?;

        commits.truncating_insert(
            UFOsCommit {
                cursor: Cursor::from_raw_u64(101),
                did: Did::new("did:plc:whatever".to_string()).unwrap(),
                rkey: RecordKey::new("rkey-asdf-b".to_string()).unwrap(),
                rev: "rev-asdg".to_string(),
                action: CommitAction::Put(PutAction {
                    record: RawValue::from_string("{}".to_string())?,
                    is_update: false,
                }),
            },
            &[0u8; 16],
        )?;

        commits.truncating_insert(
            UFOsCommit {
                cursor: Cursor::from_raw_u64(102),
                did: Did::new("did:plc:whatever".to_string()).unwrap(),
                rkey: RecordKey::new("rkey-asdf-c".to_string()).unwrap(),
                rev: "rev-asdh".to_string(),
                action: CommitAction::Put(PutAction {
                    record: RawValue::from_string("{}".to_string())?,
                    is_update: false,
                }),
            },
            &[0u8; 16],
        )?;

        assert_eq!(commits.creates, 2);
        assert_eq!(commits.deletes, 1);
        assert_eq!(commits.dids_estimate.estimate(), 1);
        assert_eq!(commits.commits.len(), 2);

        let mut found_first = false;
        let mut found_last = false;
        let mut found_delete = false;
        for commit in commits.commits {
            match commit.rev.as_ref() {
                "rev-asdg" => {
                    found_first = true;
                }
                "rev-asdh" => {
                    found_last = true;
                }
                _ => {}
            }
            if let CommitAction::Cut = commit.action {
                found_delete = true;
            }
        }
        assert!(!found_first);
        assert!(found_last);
        assert!(found_delete);

        Ok(())
    }

    #[test]
    fn test_truncating_insert_maxes_out_deletes() -> anyhow::Result<()> {
        let mut commits: CollectionCommits<2> = Default::default();

        commits
            .truncating_insert(
                UFOsCommit {
                    cursor: Cursor::from_raw_u64(100),
                    did: Did::new("did:plc:whatever".to_string()).unwrap(),
                    rkey: RecordKey::new("rkey-asdf-a".to_string()).unwrap(),
                    rev: "rev-asdf".to_string(),
                    action: CommitAction::Cut,
                },
                &[0u8; 16],
            )
            .unwrap();

        // this create will just be discarded
        commits
            .truncating_insert(
                UFOsCommit {
                    cursor: Cursor::from_raw_u64(80),
                    did: Did::new("did:plc:whatever".to_string()).unwrap(),
                    rkey: RecordKey::new("rkey-asdf-zzz".to_string()).unwrap(),
                    rev: "rev-asdzzz".to_string(),
                    action: CommitAction::Put(PutAction {
                        record: RawValue::from_string("{}".to_string())?,
                        is_update: false,
                    }),
                },
                &[0u8; 16],
            )
            .unwrap();

        commits
            .truncating_insert(
                UFOsCommit {
                    cursor: Cursor::from_raw_u64(101),
                    did: Did::new("did:plc:whatever".to_string()).unwrap(),
                    rkey: RecordKey::new("rkey-asdf-b".to_string()).unwrap(),
                    rev: "rev-asdg".to_string(),
                    action: CommitAction::Cut,
                },
                &[0u8; 16],
            )
            .unwrap();

        let res = commits.truncating_insert(
            UFOsCommit {
                cursor: Cursor::from_raw_u64(102),
                did: Did::new("did:plc:whatever".to_string()).unwrap(),
                rkey: RecordKey::new("rkey-asdf-c".to_string()).unwrap(),
                rev: "rev-asdh".to_string(),
                action: CommitAction::Cut,
            },
            &[0u8; 16],
        );

        assert!(res.is_err());
        let overflowed = match res {
            Err(BatchInsertError::BatchFull(c)) => c,
            e => panic!("expected overflow but a different error happened: {e:?}"),
        };
        assert_eq!(overflowed.rev, "rev-asdh");

        Ok(())
    }
}
