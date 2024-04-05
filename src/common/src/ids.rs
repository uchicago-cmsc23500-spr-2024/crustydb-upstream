use std::fmt;
use std::sync::atomic::{AtomicU16, AtomicU32, AtomicU64, Ordering};

use crate::Field;

static TXN_COUNTER: AtomicU64 = AtomicU64::new(0);
pub type TidType = u64;

/// Permissions for locks.
pub enum Permissions {
    ReadOnly,
    ReadWrite,
}

/// Implementation of transaction id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionId {
    /// Id of transaction.
    id: TidType,
}

impl TransactionId {
    /// Creates a new transaction id.
    pub fn new() -> Self {
        Self {
            id: TXN_COUNTER.fetch_add(1, Ordering::SeqCst),
        }
    }

    /// Returns the transaction id.
    pub fn id(&self) -> u64 {
        self.id
    }
}

impl Default for TransactionId {
    fn default() -> Self {
        TransactionId::new()
    }
}

/// The type for the container ID and the associated atomic type (for use within a Storage Manager)
// pub type ContainerId = u16;
pub type AtomicContainerId = AtomicU16;
pub type SegmentId = u8;
pub type PageId = u16;
pub type SlotId = u16;

/// For field changes
pub type TupleAssignments = Vec<(usize, Field)>;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[allow(dead_code)]
/// The things that can be saved and maintained in the database
pub enum StateType {
    HashTable,
    BaseTable,
    MatView,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StateMeta {
    /// The type of state being stored
    pub state_type: StateType,
    /// The ID for storing this container
    pub id: ContainerId,
    /// An optional name
    pub name: Option<String>,
    /// The last time this was updated if at all
    pub last_update: Option<LogicalTimeStamp>,
    /// Containers needed for the query plan to update this state
    pub dependencies: Option<Vec<ContainerId>>,
}

/// Holds information to find a record or value's bytes in a storage manager.
/// Depending on storage manager (SM), various elements may be used.
/// For example a disk-based SM may use pages to store the records, where
/// a main-memory based storage manager may not.
/// It is up to a particular SM to determine how and when to use
#[derive(PartialEq, Clone, Copy, Eq, Hash, Serialize, Deserialize)]
pub struct ValueId {
    /// The source of the value. This could represent a table, index, or other data structure.
    /// All values stored must be associated with a container that is created by the storage manager.
    pub container_id: ContainerId,
    /// An optional segment or partition ID
    pub segment_id: Option<SegmentId>,
    /// An optional page id
    pub page_id: Option<PageId>,
    /// An optional slot id. This could represent a physical or logical ID.
    pub slot_id: Option<SlotId>,
}

impl ValueId {
    pub fn new(container_id: ContainerId) -> Self {
        ValueId {
            container_id,
            segment_id: None,
            page_id: None,
            slot_id: None,
        }
    }

    pub fn new_page(container_id: ContainerId, page_id: PageId) -> Self {
        ValueId {
            container_id,
            segment_id: None,
            page_id: Some(page_id),
            slot_id: None,
        }
    }

    pub fn new_slot(container_id: ContainerId, page_id: PageId, slot_id: SlotId) -> Self {
        ValueId {
            container_id,
            segment_id: None,
            page_id: Some(page_id),
            slot_id: Some(slot_id),
        }
    }
}

impl fmt::Debug for ValueId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut buf: String = format!("<c_id:{}", self.container_id);
        if self.segment_id.is_some() {
            buf.push_str(",seg_id:");
            buf.push_str(&self.segment_id.unwrap().to_string());
        }
        if self.page_id.is_some() {
            buf.push_str(",p_id:");
            buf.push_str(&self.page_id.unwrap().to_string());
        }
        if self.slot_id.is_some() {
            buf.push_str(",slot_id:");
            buf.push_str(&self.slot_id.unwrap().to_string());
        }
        buf.push('>');
        write!(f, "{}", buf)
    }
}

pub struct Lsn {
    pub page_id: PageId,
    pub slot_id: SlotId,
}

/// Stuff delta storage manager
pub type LogicalTimeStamp = u32;
pub type AtomicTimeStamp = AtomicU32;
pub type ContainerId = u16;
