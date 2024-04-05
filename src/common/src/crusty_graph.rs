use std::collections::HashMap;
use std::collections::HashSet;

use crate::error::CrustyError;

/// Generic Graph struct
/// Implementation based on:
/// https://smallcultfollowing.com/babysteps/blog/2015/04/06/modeling-graphs-in-rust-using-vector-indices/
/// https://docs.rs/petgraph/0.4.13/petgraph/graph/struct.Graph.html#method.node_weight

/// Index of node in internal graph representation, used as a node identifier
pub type NodeIndex = usize;
/// Index of edge in internal graph representation, used as an edge identifier
pub type EdgeIndex = usize;

/// Represents a node in the graph
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node<T> {
    /// Data that is associated with the node
    data: T,
    /// Index of the first edge in a linked list of edges connected to the node.
    outgoing_edge: Option<EdgeIndex>,
}

impl<T> Node<T> {
    /// Get the data of a node.
    pub fn data(&self) -> &T {
        &self.data
    }
}

/// Represents an edge from `source` to `target` in the graph. Next references the next edge in inked list of all edges connected to source.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Edge {
    /// Source of the edge.
    source: NodeIndex,
    /// Destination of the edge.
    target: NodeIndex,
    /// Next edge in the linkedlist of edges.
    next: Option<EdgeIndex>,
}

impl Edge {
    /// Returns the edge source.
    pub fn source(&self) -> NodeIndex {
        self.source
    }

    /// Returns the edge destination.
    pub fn target(&self) -> NodeIndex {
        self.target
    }
}

/// Generic graph implementation for logical and physical plans.
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct CrustyGraph<T> {
    /// Vec of all nodes in the graph.
    /// A NodeIndex references a position in this vec.
    nodes: Vec<Node<T>>,
    /// Vec of all edges in the graph.
    /// A EdgeIndex references a position in this vec.
    edges: Vec<Edge>,
}

impl<T> CrustyGraph<T> {
    /// Creates a new CrustyGraph.
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// Gets the Edge struct corresponding to a given EdgeIndex
    ///
    /// # Arguments
    ///
    /// * `edge` - Index of the edge to look for.
    ///
    /// # Panics
    ///
    /// if edge is not present in the graph
    pub fn edge(&self, edge: EdgeIndex) -> &Edge {
        &self.edges[edge]
    }

    /// Gets the Node struct corresponding to a given NodeIndex
    ///
    /// # Arguments
    ///
    /// * `node` - Index of the node to look for.
    ///
    /// # Panics
    ///
    /// if node is not present in the graph
    pub fn node(&self, node: NodeIndex) -> &Node<T> {
        &self.nodes[node]
    }

    /// Adds a node with associated data to the graph and returns the index of the new node.
    ///
    /// # Arguments
    ///
    /// * `data` - Data for the node to add to the graph.
    pub fn add_node(&mut self, data: T) -> NodeIndex {
        let index = self.nodes.len();
        let node = Node {
            data,
            outgoing_edge: None,
        };
        self.nodes.push(node);
        index
    }

    /// Check if a node with the given index exists.
    ///
    /// # Arguments
    ///
    /// * `node` - Index of the node to look for.
    fn node_index_exists(&self, node: NodeIndex) -> bool {
        self.nodes.get(node).is_some()
    }

    /// Adds edge from source to target.
    ///
    /// Edges are added to the front of the linked list of edges connected to source.
    ///
    /// # Arguments
    ///
    /// * `source` - Source node of the edge.
    /// * `target` - Target node of the edge.
    ///
    /// # Panics
    ///
    /// if source or target are not valid NodeIndex's
    pub fn add_edge(&mut self, source: NodeIndex, target: NodeIndex) {
        if !self.node_index_exists(source) {
            panic!("Source node does not exist");
        }

        if !self.node_index_exists(target) {
            panic!("Target node does not exist");
        }
        let node = &mut self.nodes[source];
        let index = self.edges.len();
        let edge = Edge {
            source,
            target,
            next: node.outgoing_edge,
        };
        self.edges.push(edge);
        node.outgoing_edge = Some(index);
    }

    /// Returns an iterator over all NodeIndex's that have an edge from source.
    ///
    /// Edges iterated in the reverse order of how they were added.
    ///
    /// # Arguments
    ///
    /// * `source` - Source node to start iterating the edges over.
    // TODO(williamma12): Check if these lifetimes are necessary or not.
    #[allow(clippy::needless_lifetimes)]
    pub fn edges<'a>(&'a self, source: NodeIndex) -> impl Iterator<Item = NodeIndex> + 'a {
        Edges::new(self, source)
    }

    /// Access the data for a node
    pub fn node_data(&self, node: NodeIndex) -> Option<&T> {
        self.nodes.get(node).map(|n| &n.data)
    }

    /// Iterator over all nodes in the graph.
    ///
    /// Iterates over NodeIndex's and their corresponding Node structs. Returned iterator shares lifetime of self.
    #[allow(clippy::needless_lifetimes)]
    pub fn node_references<'a>(&'a self) -> impl Iterator<Item = (NodeIndex, &Node<T>)> + 'a {
        self.nodes.iter().enumerate()
    }

    /// Iterator over all edges present in the graph>
    ///
    /// Iterator shares lifetime of self.
    #[allow(clippy::needless_lifetimes)]
    pub fn edge_references<'a>(&'a self) -> impl Iterator<Item = &Edge> + 'a {
        self.edges.iter()
    }

    /// Number of nodes in the entire graph.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Number of edges in the entire graph.
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    /// Returns Some(topological order) if graph has a topological order
    /// Returns None if it does not
    pub fn topological_order(&self) -> Option<Vec<NodeIndex>> {
        let mut num_incoming: HashMap<NodeIndex, u32> = HashMap::new();
        let mut none_incoming: Vec<NodeIndex> = Vec::new();
        let mut res: Vec<NodeIndex> = Vec::new();

        for (node, _) in self.node_references() {
            num_incoming.insert(node, 0);
        }

        for (node, _) in self.node_references() {
            for child in self.edges(node) {
                if let Some(num) = num_incoming.get_mut(&child) {
                    *num += 1;
                }
            }
        }

        for node in num_incoming.keys() {
            if let Some(num) = num_incoming.get(node) {
                if *num == 0 {
                    none_incoming.push(*node);
                }
            }
        }

        while let Some(next) = none_incoming.pop() {
            res.push(next);

            for child in self.edges(next) {
                if let Some(num) = num_incoming.get_mut(&child) {
                    if *num == 1 {
                        num_incoming.remove(&child);
                        none_incoming.push(child);
                    } else {
                        *num -= 1;
                    }
                }
            }
        }

        if res.len() == self.node_count() {
            Some(res)
        } else {
            None
        }
    }

    /// Checks if the graph is cycle free
    pub fn cycle_free(&self) -> bool {
        self.topological_order().is_some()
    }

    /// Checks if all nodes in graph are reachable from input node
    /// Raises error if the input node is not in the graph
    pub fn all_reachable_from_node(&self, node: NodeIndex) -> Result<bool, CrustyError> {
        if !self.node_index_exists(node) {
            return Err(CrustyError::CrustyError(String::from(
                "tried to check if graph is all reachable from invalid node",
            )));
        }
        let mut visited: HashSet<NodeIndex> = HashSet::new();
        let mut todo: Vec<NodeIndex> = vec![node];

        while let Some(next) = todo.pop() {
            for child in self.edges(next) {
                if !visited.contains(&child) {
                    todo.push(child);
                }
            }
            visited.insert(next);
        }
        Ok(visited.len() == self.node_count())
    }
}

/// Iterator over all edges from a source node.
/// Lifetime tied to the lifetime of the CrustyGraph graph.
pub struct Edges<'a, T> {
    graph: &'a CrustyGraph<T>,
    edge: Option<EdgeIndex>,
}

impl<'a, T> Edges<'a, T> {
    /// Create an iterator for all the edges with the given source node.
    ///
    /// # Arguments
    ///
    /// * `source` - Source node's edges to iterate over.
    pub fn new(graph: &'a CrustyGraph<T>, source: NodeIndex) -> Self {
        Self {
            graph,
            edge: graph.node(source).outgoing_edge,
        }
    }
}
impl<'a, T> Iterator for Edges<'a, T> {
    type Item = NodeIndex;

    fn next(&mut self) -> Option<NodeIndex> {
        self.edge.map(|i| {
            let e = self.graph.edge(i);
            self.edge = e.next;
            e.target
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn add_node() {
        let mut graph = CrustyGraph::new();

        for data in &[0, 1, 100] {
            let i = graph.add_node(data);
            let node = &graph.nodes[i];
            assert_eq!(*node.data(), data);
            assert!(node.outgoing_edge.is_none());
        }
    }

    #[test]
    fn add_edge() {
        let mut graph = CrustyGraph::<i32>::new();
        let parent = graph.add_node(0);
        let mut edge_count = 0;
        assert_eq!(graph.edge_count(), edge_count);

        for d in 1..4 {
            let c = graph.add_node(d);
            graph.add_edge(parent, c);
            edge_count += 1;
            assert_eq!(graph.edge_count(), edge_count);
            let pnode = graph.node(parent);
            let e_idx = pnode.outgoing_edge.unwrap();
            let edge = &graph.edges[e_idx];
            assert_eq!(edge.source(), parent);
            assert_eq!(edge.target(), c);
        }
    }

    #[test]
    fn edges() {
        // Single parent
        let mut graph = CrustyGraph::<i32>::new();
        let parent = graph.add_node(0);
        let mut children = Vec::new();
        for v in 0..3 {
            let i = graph.add_node(v);
            graph.add_edge(parent, i);
            children.push(i);
        }

        for (actual, expected) in graph.edges(parent).zip(children.iter().rev()) {
            assert_eq!(actual, *expected);
        }

        // multiple parents
        let p1 = graph.add_node(0);
        let p2 = graph.add_node(1);
        let mut c1 = Vec::new();
        let mut c2 = Vec::new();

        for v in 0..3 {
            let i1 = graph.add_node(v);
            graph.add_edge(p1, i1);
            c1.push(i1);

            let i2 = graph.add_node(v);
            graph.add_edge(p2, i2);
            c2.push(i2);
        }

        for (actual, expected) in graph.edges(p1).zip(c1.iter().rev()) {
            assert_eq!(actual, *expected);
        }

        for (actual, expected) in graph.edges(p2).zip(c2.iter().rev()) {
            assert_eq!(actual, *expected);
        }
    }

    #[test]
    fn node() {
        let mut graph = CrustyGraph::<i32>::new();

        for expected in 0..5 {
            let i = graph.add_node(expected);
            assert_eq!(graph.node(i).data, expected);
        }
    }

    #[test]
    fn node_references() {
        let mut graph = CrustyGraph::<i32>::new();
        let mut indices = Vec::new();
        for v in 0..3 {
            let i = graph.add_node(v);
            indices.push(i);
        }

        for ((actual_i, n), expected_i) in graph.node_references().zip(indices.iter()) {
            assert_eq!(actual_i, *expected_i);
            assert_eq!(n, graph.node(*expected_i));
        }
    }

    #[test]
    fn edge_referneces() {
        let mut graph = CrustyGraph::<i32>::new();
        let p1 = graph.add_node(0);
        let p2 = graph.add_node(1);

        for v in 0..3 {
            let i1 = graph.add_node(v);
            graph.add_edge(p1, i1);

            let i2 = graph.add_node(v);
            graph.add_edge(p2, i2);
        }

        for (actual, expected) in graph.edge_references().zip(graph.edges.iter()) {
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn node_count() {
        let mut graph = CrustyGraph::new();
        let expected: usize = 5;
        for i in 0..expected {
            assert_eq!(graph.node_count(), i);
            graph.add_node(i);
        }
        assert_eq!(graph.node_count(), expected);
    }

    #[test]
    fn edge_count() {
        let mut graph = CrustyGraph::<i32>::new();
        let p1 = graph.add_node(0);
        let p2 = graph.add_node(1);
        let mut expected = 0;
        assert_eq!(graph.edge_count(), expected);
        for v in 0..3 {
            let i1 = graph.add_node(v);
            graph.add_edge(p1, i1);
            expected += 1;
            assert_eq!(graph.edge_count(), expected);

            let i2 = graph.add_node(v);
            graph.add_edge(p2, i2);
            expected += 1;
            assert_eq!(graph.edge_count(), expected);
        }
    }

    fn test_graph1() -> CrustyGraph<i32> {
        let mut graph = CrustyGraph::<i32>::new();
        let v1 = graph.add_node(0);
        let v2 = graph.add_node(0);
        let v3 = graph.add_node(0);
        let v4 = graph.add_node(0);

        graph.add_edge(v1, v3);
        graph.add_edge(v3, v2);
        graph.add_edge(v1, v4);
        graph.add_edge(v2, v4);

        graph
    }

    fn test_graph2() -> CrustyGraph<i32> {
        let mut graph = CrustyGraph::<i32>::new();
        let v1 = graph.add_node(0);
        let v2 = graph.add_node(0);
        let v3 = graph.add_node(0);
        let v4 = graph.add_node(0);
        let v5 = graph.add_node(0);

        graph.add_edge(v1, v2);
        graph.add_edge(v2, v3);
        graph.add_edge(v3, v4);
        graph.add_edge(v4, v1);
        graph.add_edge(v5, v2);

        graph
    }

    #[test]
    fn topological_order() {
        let g1 = test_graph1();
        assert_eq!(g1.topological_order(), Some(vec![0, 2, 1, 3]));

        let g2 = test_graph2();
        assert_eq!(g2.topological_order(), None);
    }

    #[test]
    fn cycle_free() {
        let g1 = test_graph1();
        assert!(g1.cycle_free());

        let g2 = test_graph2();
        assert!(!g2.cycle_free());
    }

    #[test]
    fn all_reachable_from_node() {
        let g1 = test_graph1();
        assert!(g1.all_reachable_from_node(0).unwrap());
        assert!(!g1.all_reachable_from_node(1).unwrap());
        assert!(!g1.all_reachable_from_node(2).unwrap());
        assert!(!g1.all_reachable_from_node(3).unwrap());

        let g2 = test_graph2();
        assert!(!g2.all_reachable_from_node(0).unwrap());
        assert!(!g2.all_reachable_from_node(1).unwrap());
        assert!(!g2.all_reachable_from_node(2).unwrap());
        assert!(!g2.all_reachable_from_node(3).unwrap());
        assert!(g2.all_reachable_from_node(4).unwrap());
    }
}
