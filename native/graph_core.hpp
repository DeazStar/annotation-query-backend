#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

using namespace std;
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;

struct Node {
    string id;
    unordered_map<string, py::object> attrs;
};

struct Edge {
    string id;
    string source;
    string target;
    string edge_id;
    string label;
    unordered_map<string, py::object> attrs;
};

struct Graph {
    vector<Node> nodes;
    vector<Edge> edges;
};

// Internal signature types
struct Connection {
    string edge_id;
    bool is_source;
    vector<string> target_nodes; // sorted

    bool operator<(const Connection& other) const {
        if (is_source != other.is_source) return is_source < other.is_source;
        if (edge_id != other.edge_id) return edge_id < other.edge_id;
        return target_nodes < other.target_nodes;
    }
};

// Function prototypes
string generate_nanoid();

Graph group_node_only(const Graph& graph, const py::dict& request);
pair<unordered_map<string, unordered_map<string, py::dict>>, unordered_map<string, py::dict>> get_node_to_connections_map(const Graph& graph);
Graph collapse_nodes(const Graph& graph);
Graph collapse_node_nx(const Graph& graph);
py::dict convert_to_graph_json(const Graph& graph, bool allow_data = true);
Graph group_into_parents(Graph graph);
Graph group_graph(const Graph& graph);
Graph break_grouping(const Graph& graph);
Graph collapse_node_nx_location(const Graph& graph);
vector<Graph> build_subgraph_nx(const Graph& graph);
