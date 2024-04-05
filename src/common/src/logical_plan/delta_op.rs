use crate::ids::ContainerId;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReadDeltasNode {
    pub object_id: ContainerId,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WriteDeltasNode {
    pub object_id: ContainerId,
}
