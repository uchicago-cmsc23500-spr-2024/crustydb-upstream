use std::collections::HashMap;
use std::default::Default;
use std::fmt;

use serde_json::{json, Value};

use crate::crusty_graph::{CrustyGraph, Edge, Node, NodeIndex};
use crate::error::CrustyError;

pub use delta_op::{ReadDeltasNode, WriteDeltasNode};
pub use logical_op::*;

use crate::ast_expr::AstExpr;
use crate::prelude::ContainerId;

mod delta_op;
mod logical_op;

/// OpIndex is used to identify nodes in the LogicalPlan.
pub type OpIndex = NodeIndex;

/// A LogicalOp represents a relational operation present in a logical query plan>
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum LogicalOp {
    Scan(ScanNode),
    Project(ProjectNode),
    Filter(FilterNode),
    Aggregate(AggregateNode),
    Join(JoinNode),
    CrossProduct(CrossProductNode),
    Sort(SortNode),
    ReadDeltas(ReadDeltasNode),
    WriteDeltas(WriteDeltasNode),
    Update(UpdateNode),
}

/// Graph where nodes represent logical operations and edges represent the flow of data.
pub struct LogicalPlan {
    /// Graph of the logical plan.
    dataflow: CrustyGraph<LogicalOp>,
    /// The root represents final output operation. Root does not work if the graph contains any unconnected components.
    root: Option<OpIndex>,
}

impl Default for LogicalPlan {
    fn default() -> Self {
        Self::new()
    }
}

impl LogicalPlan {
    /// Creates an empty logical plan.
    pub fn new() -> Self {
        Self {
            dataflow: CrustyGraph::new(),
            root: None,
        }
    }

    pub fn merge(&mut self, new_root: LogicalOp, other: LogicalPlan) {
        assert!(matches!(
            new_root,
            LogicalOp::Join(_) | LogicalOp::CrossProduct(_)
        ));

        let root_left = self.root.unwrap();
        let root_right = other.root.unwrap();

        let mut node_map = HashMap::new();
        for (i, node) in other.dataflow.node_references() {
            let new_node = self.add_node(node.data().clone());
            node_map.insert(i, new_node);
        }
        for edge in other.dataflow.edge_references() {
            let source = edge.source();
            let target = edge.target();
            let new_source = node_map.get(&source).unwrap();
            let new_target = node_map.get(&target).unwrap();
            self.add_edge(*new_source, *new_target);
        }
        let new_root_idx = self.add_node(new_root);
        // Right child is always the first edge to be added.
        self.add_edge(new_root_idx, *node_map.get(&root_right).unwrap());
        self.add_edge(new_root_idx, root_left);
    }

    /// Adds a node with an associated LogicalOp to the logical plan and returns the index of the added node.
    ///
    /// # Arguments
    ///
    /// * `operator` - Operator to add to the logical plan.
    pub fn add_node(&mut self, operator: LogicalOp) -> OpIndex {
        let index = self.dataflow.add_node(operator);
        if self.root.is_none() {
            self.root = Some(index)
        }
        index
    }

    /// Adds from source to target.
    ///
    /// In the logical plan representation data flows from target to source.
    ///
    /// # Arguments
    ///
    /// * `source` - Data consumer, closer to root. e.g. root projection
    /// * `target` - Data producer, closer to leaf. e.g. scan
    pub fn add_edge(&mut self, source: OpIndex, target: OpIndex) {
        if source == target {
            panic!("Attempted to add edge from node to itself");
        }
        if let Some(index) = self.root {
            if index == target {
                self.root = Some(source);
            }
        }
        self.dataflow.add_edge(source, target);
    }

    /// Returns an iterator over all nodes that 'from' has an edge to.
    ///
    /// # Arguments
    ///
    /// * `from` - Node to get the edges of.
    // TODO(williamma12): Check if these lifetimes are necessary or not.
    #[allow(clippy::needless_lifetimes)]
    pub fn edges<'a>(&'a self, from: OpIndex) -> impl Iterator<Item = NodeIndex> + 'a {
        self.dataflow.edges(from)
    }

    /// Gets the index of the root node, if such a node is present.
    ///
    /// The root node represents the final output operation in the logical plan.
    pub fn root(&self) -> Option<OpIndex> {
        self.root
    }

    /// Returns the LogicalOperation associated with a node.
    ///
    /// # Arguments
    ///
    /// * `index` - Index of the node to get the logical operation of.
    pub fn get_operator(&self, index: OpIndex) -> Option<&LogicalOp> {
        self.dataflow.node_data(index)
    }

    /// Iterator over all nodes in the graph.
    ///
    /// Iterates over NodeIndex's and their corresponding Node structs. Returned iterator shares lifetime of self.
    #[allow(clippy::needless_lifetimes)]
    pub fn node_references<'a>(
        &'a self,
    ) -> impl Iterator<Item = (NodeIndex, &Node<LogicalOp>)> + 'a {
        self.dataflow.node_references()
    }

    /// Iterator over all edges present in the graph>
    ///
    /// Iterator shares lifetime of self.
    #[allow(clippy::needless_lifetimes)]
    pub fn edge_references<'a>(&'a self) -> impl Iterator<Item = &Edge> + 'a {
        self.dataflow.edge_references()
    }

    /// Returns the total number of nodes present in the graph.
    pub fn node_count(&self) -> usize {
        self.dataflow.node_count()
    }

    /// Returns the total number of edges present in the graph.
    pub fn edge_count(&self) -> usize {
        self.dataflow.edge_count()
    }

    /// Serializes the Logical Plan as json.
    pub fn to_json(&self) -> serde_json::Value {
        let mut node_map = HashMap::new();
        let mut edge_map = HashMap::new();
        for (i, node) in self.dataflow.node_references() {
            node_map.insert(i, node.data());
        }
        for edge in self.dataflow.edge_references() {
            let source = edge.source();
            let targets = edge_map.entry(source).or_insert_with(Vec::new);
            targets.push(edge.target().to_string());
        }
        json!({"nodes":node_map,
                      "edges":edge_map,
                      "root":self.root.map(|i| i.to_string())})
    }

    fn map_crusty_err<T>(
        result: serde_json::Result<T>,
        err: CrustyError,
    ) -> Result<T, CrustyError> {
        match result {
            Ok(res) => Ok(res),
            _ => Err(err),
        }
    }

    /// De-Serializes a json representation of the Logical Plan created in to_json
    pub fn from_json(json: &str) -> Result<Self, CrustyError> {
        let malformed_err =
            CrustyError::CrustyError(String::from("Malformatted logical plan json"));
        let v: Value =
            LogicalPlan::map_crusty_err(serde_json::from_str(json), malformed_err.clone())?;
        let nodes: HashMap<String, LogicalOp> = LogicalPlan::map_crusty_err(
            serde_json::from_value(v["nodes"].clone()),
            malformed_err.clone(),
        )?;
        let edges: HashMap<String, Vec<String>> = LogicalPlan::map_crusty_err(
            serde_json::from_value(v["edges"].clone()),
            malformed_err.clone(),
        )?;
        let root: Option<String> = LogicalPlan::map_crusty_err(
            serde_json::from_value(v["root"].clone()),
            malformed_err.clone(),
        )?;
        let mut graph_map = HashMap::new();
        let mut plan = LogicalPlan::new();
        for (i, val) in nodes.iter() {
            let node = plan.add_node(val.clone());
            graph_map.insert(i, node);
        }

        if let Some(i) = root {
            let root_node = graph_map.get(&i).ok_or_else(|| malformed_err.clone())?;
            plan.root = Some(*root_node);
        }

        for (source, targets) in edges.iter() {
            let source_node = graph_map.get(source).ok_or_else(|| malformed_err.clone())?;
            for target in targets {
                let target_node = graph_map
                    .get(&target.to_string())
                    .ok_or_else(|| malformed_err.clone())?;
                plan.add_edge(*source_node, *target_node);
            }
        }

        if !plan.cycle_free() {
            return Err(CrustyError::CrustyError(String::from(
                "Logical Plan loaded from json contains a cycle",
            )));
        }

        if !plan.all_reachable_from_root()? {
            return Err(CrustyError::CrustyError(String::from(
                "Logical Plan loaded from json contains nodes not reachable from root",
            )));
        }

        Ok(plan)
    }

    /// Checks if the logical plan has a cycle
    /// if this has a cycle, the query could run forever
    pub fn cycle_free(&self) -> bool {
        self.dataflow.cycle_free()
    }

    /// Checks if all nodes in the operator graph are reachable from the root
    /// Raises error if the logical plan has no root
    pub fn all_reachable_from_root(&self) -> Result<bool, CrustyError> {
        match self.root() {
            Some(node) => self.dataflow.all_reachable_from_node(node),
            None => Err(CrustyError::CrustyError(String::from(
                "Logical plan loaded from json has no root",
            ))),
        }
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_json())
    }
}

impl LogicalPlan {
    pub fn add_scan_node(
        &mut self,
        container_id: ContainerId,
        filter: Option<AstExpr>,
        projection: Option<Vec<AstExpr>>,
    ) -> OpIndex {
        let scan = LogicalOp::Scan(ScanNode {
            container_id,
            filter,
            projection,
        });
        let scan_idx = self.dataflow.add_node(scan);
        if self.root.is_none() {
            self.root = Some(scan_idx);
        }
        scan_idx
    }

    pub fn add_filter_node(&mut self, predicate: AstExpr, child_idx: Option<OpIndex>) -> OpIndex {
        let filter_node = LogicalOp::Filter(FilterNode { predicate });
        let filter_idx = self.dataflow.add_node(filter_node);

        if let Some(child_index) = child_idx {
            self.dataflow.add_edge(filter_idx, child_index);
        } else if let Some(root_idx) = self.root {
            self.dataflow.add_edge(filter_idx, root_idx);
            self.root = Some(filter_idx);
        }

        filter_idx
    }

    pub fn add_projection_node(
        &mut self,
        identifiers: Vec<AstExpr>,
        child_idx: Option<OpIndex>,
    ) -> OpIndex {
        let projection_node = LogicalOp::Project(ProjectNode { identifiers });
        let projection_idx = self.dataflow.add_node(projection_node);

        if let Some(child_index) = child_idx {
            self.dataflow.add_edge(projection_idx, child_index);
        } else if let Some(root_idx) = self.root {
            self.dataflow.add_edge(projection_idx, root_idx);
            self.root = Some(projection_idx);
        }

        projection_idx
    }

    pub fn add_join_node(
        &mut self,
        eqs: Vec<(AstExpr, AstExpr)>,
        filter: Option<AstExpr>,
        left_idx: OpIndex,
        right_idx: OpIndex,
    ) -> OpIndex {
        let join = LogicalOp::Join(JoinNode { eqs, filter });
        let join_idx = self.dataflow.add_node(join);
        self.dataflow.add_edge(join_idx, left_idx);
        self.dataflow.add_edge(join_idx, right_idx);
        self.root = Some(join_idx);
        join_idx
    }

    pub fn add_cross_product_node(
        &mut self,
        filter: Option<AstExpr>,
        left_idx: OpIndex,
        right_idx: OpIndex,
    ) -> OpIndex {
        let cross_product = LogicalOp::CrossProduct(CrossProductNode { filter });
        let cross_product_idx = self.dataflow.add_node(cross_product);
        self.dataflow.add_edge(cross_product_idx, left_idx);
        self.dataflow.add_edge(cross_product_idx, right_idx);
        self.root = Some(cross_product_idx);
        cross_product_idx
    }

    pub fn add_agg_node(
        &mut self,
        fields: Vec<AstExpr>,
        group_by: Vec<AstExpr>,
        having: Option<AstExpr>,
        child_idx: Option<OpIndex>,
    ) -> OpIndex {
        let agg_node = LogicalOp::Aggregate(AggregateNode {
            fields,
            group_by,
            having,
        });
        let agg_idx = self.dataflow.add_node(agg_node);

        if let Some(child_index) = child_idx {
            self.dataflow.add_edge(agg_idx, child_index);
        } else if let Some(root_idx) = self.root {
            self.dataflow.add_edge(agg_idx, root_idx);
            self.root = Some(agg_idx);
        }

        agg_idx
    }
}

#[cfg(test)]
mod test {
    use core::panic;

    use super::*;

    use crate::ids::ContainerId;

    #[test]
    fn test_new() {
        let lp = LogicalPlan::new();
        assert_eq!(lp.node_count(), 0);
        assert_eq!(lp.edge_count(), 0);
        assert_eq!(lp.root, None);
    }

    #[test]
    fn test_add_node() {
        let count = 10;
        let mut lp = LogicalPlan::new();
        for _ in 0..count {
            lp.add_node(LogicalOp::Scan(ScanNode {
                container_id: 1,
                filter: None,
                projection: None,
            }));
        }
        assert_eq!(lp.node_count(), count);
    }

    #[test]
    fn test_add_edge() {
        let count = 10;
        let mut lp = LogicalPlan::new();
        let mut prev = lp.add_node(LogicalOp::Scan(ScanNode {
            container_id: 0,
            filter: None,
            projection: None,
        }));
        for i in 0..count {
            let curr = lp.add_node(LogicalOp::Scan(ScanNode {
                container_id: i as ContainerId,
                filter: None,
                projection: None,
            }));
            lp.add_edge(curr, prev);
            prev = curr;
        }
        assert_eq!(lp.root, Some(prev));
        assert_eq!(lp.edge_count(), count);
    }

    #[test]
    fn test_add_two_edges() {
        let mut lp = LogicalPlan::new();
        let parent = lp.add_node(LogicalOp::Scan(ScanNode {
            container_id: 0,
            filter: None,
            projection: None,
        }));
        let child1 = lp.add_node(LogicalOp::Scan(ScanNode {
            container_id: 1,
            filter: None,
            projection: None,
        }));
        let child2 = lp.add_node(LogicalOp::Scan(ScanNode {
            container_id: 2,
            filter: None,
            projection: None,
        }));
        lp.add_edge(parent, child1);
        lp.add_edge(parent, child2);
        assert_eq!(lp.edge_count(), 2);
    }

    #[test]
    fn test_edges() {
        let mut lp = LogicalPlan::new();
        let parent = lp.add_node(LogicalOp::Scan(ScanNode {
            container_id: 0,
            filter: None,
            projection: None,
        }));
        let child1 = lp.add_node(LogicalOp::Scan(ScanNode {
            container_id: 1,
            filter: None,
            projection: None,
        }));
        let child2 = lp.add_node(LogicalOp::Scan(ScanNode {
            container_id: 2,
            filter: None,
            projection: None,
        }));
        lp.add_edge(parent, child1);
        lp.add_edge(parent, child2);
        let mut edges = lp.edges(parent);
        assert_eq!(edges.next(), Some(child2));
        assert_eq!(edges.next(), Some(child1));
    }

    #[test]
    fn test_get_operator() {
        let count = 5;
        let mut nodes = Vec::new();
        let mut lp = LogicalPlan::new();
        for i in 0..count {
            let index = lp.add_node(LogicalOp::Scan(ScanNode {
                container_id: i as ContainerId,
                filter: None,
                projection: None,
            }));
            nodes.push(index);
        }

        for (i, &node) in nodes.iter().enumerate().take(count) {
            match lp.get_operator(node) {
                Some(LogicalOp::Scan(s)) => {
                    assert_eq!(i as u16, s.container_id);
                }
                _ => panic!("Incorrect operator"),
            }
        }
    }

    #[test]
    fn test_json() {
        let mut lp = LogicalPlan::new();
        let scan = lp.add_node(LogicalOp::Scan(ScanNode {
            container_id: 0,
            filter: None,
            projection: None,
        }));
        let project = lp.add_node(LogicalOp::Project(ProjectNode {
            identifiers: vec![],
        }));
        lp.add_edge(project, scan);
        let json = lp.to_json();
        debug!("{:?}", json);
        let new_lp = LogicalPlan::from_json(&json.to_string()).unwrap();
        assert_eq!(lp.dataflow.node_count(), new_lp.dataflow.node_count());
        assert_eq!(lp.dataflow.edge_count(), new_lp.dataflow.edge_count());

        let original_root = lp.dataflow.node_data(lp.root.unwrap()).unwrap();
        let new_root = new_lp.dataflow.node_data(new_lp.root.unwrap()).unwrap();
        match (original_root, new_root) {
            (LogicalOp::Project(_), LogicalOp::Project(_)) => (),
            _ => panic!("Incorrect root"),
        }
    }

    #[test]
    fn test_cycle_free() {
        let mut lp = LogicalPlan::new();
        let scan = lp.add_node(LogicalOp::Scan(ScanNode {
            container_id: 0,
            filter: None,
            projection: None,
        }));
        let project = lp.add_node(LogicalOp::Project(ProjectNode {
            identifiers: vec![],
        }));
        lp.add_edge(project, scan);
        assert!(lp.cycle_free());

        lp.add_edge(scan, project);
        assert!(!lp.cycle_free());
    }

    #[test]
    fn test_all_reachable_from_root() {
        let mut lp = LogicalPlan::new();
        assert!(lp.all_reachable_from_root().is_err());

        let scan = lp.add_node(LogicalOp::Scan(ScanNode {
            container_id: 0,
            filter: None,
            projection: None,
        }));
        assert!(lp.all_reachable_from_root().unwrap());

        let project = lp.add_node(LogicalOp::Project(ProjectNode {
            identifiers: vec![],
        }));
        assert!(!lp.all_reachable_from_root().unwrap());

        lp.add_edge(project, scan);
        assert!(lp.all_reachable_from_root().unwrap());

        lp.add_edge(scan, project);
        assert!(lp.all_reachable_from_root().unwrap());
    }
}
