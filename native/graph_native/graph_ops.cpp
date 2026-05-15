#include "graph_ops.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace graph_native {

namespace py = pybind11;


namespace {

struct NodeData {
    std::string id;
    std::string type;
    std::string name;
    std::string parent;
    py::dict data;
};

struct EdgeData {
    std::string source;
    std::string target;
    std::string label;
    std::string edge_id;
};

struct ParsedGraph {
    std::vector<NodeData> nodes;
    std::vector<EdgeData> edges;
};

struct CollapseResult {
    ParsedGraph graph;
    std::unordered_map<std::string, std::string> group_for_node;
};

struct VectorPairHash {
    std::size_t operator()(const std::vector<std::pair<std::string, std::string>>& vec) const {
        std::size_t seed = vec.size();
        for (const auto& pair : vec) {
            std::hash<std::string> hasher;
            seed ^= hasher(pair.first) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            seed ^= hasher(pair.second) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    }
};

struct SignatureHash {
    std::size_t operator()(const std::pair<std::vector<std::pair<std::string, std::string>>,
                                            std::vector<std::pair<std::string, std::string>>>& sig) const {
        VectorPairHash hasher;
        std::size_t h1 = hasher(sig.first);
        std::size_t h2 = hasher(sig.second);
        return h1 ^ (h2 + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
    }
};

class IDGenerator {
private:
    static constexpr const char* CHARSET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static constexpr int ID_LENGTH = 12;

public:
    static std::string next_id() {
        thread_local static std::random_device rd;
        thread_local static std::mt19937 gen(rd());
        thread_local static std::uniform_int_distribution<> dis(0, 61);
        
        std::string result;
        result.reserve(ID_LENGTH);
        for (int i = 0; i < ID_LENGTH; ++i) {
            result += CHARSET[dis(gen)];
        }
        return result;
    }
};

std::string next_id() { return IDGenerator::next_id(); }

std::vector<std::string> split(const std::string& text, char delimiter) {
    std::vector<std::string> parts;
    std::stringstream ss(text);
    std::string item;
    while (std::getline(ss, item, delimiter)) {
        if (!item.empty()) {
            parts.push_back(item);
        }
    }
    return parts;
}

std::string join(const std::vector<std::string>& values, const std::string& separator) {
    std::ostringstream out;
    for (std::size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            out << separator;
        }
        out << values[i];
    }
    return out.str();
}

std::string extract_middle(const std::string& edge_id) {
    std::vector<std::string> parts = split(edge_id, '_');
    if (parts.size() <= 2) {
        return edge_id;
    }
    std::vector<std::string> middle(parts.begin() + 1, parts.end() - 1);
    return join(middle, "_");
}

std::string safe_dict_get_string(const py::dict& dict, const std::string& key, const std::string& default_value = "") {
    try {
        if (!dict.contains(key.c_str())) {
            return default_value;
        }
        py::object value = dict[key.c_str()];
        if (value.is_none()) {
            return default_value;
        }
        return py::str(value);
    } catch (const py::type_error& e) {
        throw std::invalid_argument("Expected string value for key '" + key + "': " + std::string(e.what()));
    }
}

py::list safe_dict_get_list(const py::dict& dict, const std::string& key) {
    try {
        if (!dict.contains(key.c_str())) {
            return py::list();
        }
        py::object value = dict[key.c_str()];
        if (value.is_none()) {
            return py::list();
        }
        return py::reinterpret_borrow<py::list>(value);
    } catch (const py::type_error& e) {
        throw std::invalid_argument("Expected list value for key '" + key + "': " + std::string(e.what()));
    }
}

py::dict safe_dict_get_dict(const py::dict& dict, const std::string& key) {
    try {
        if (!dict.contains(key.c_str())) {
            return py::dict();
        }
        py::object value = dict[key.c_str()];
        if (value.is_none()) {
            return py::dict();
        }
        return py::reinterpret_borrow<py::dict>(value);
    } catch (const py::type_error& e) {
        throw std::invalid_argument("Expected dict value for key '" + key + "': " + std::string(e.what()));
    }
}

void validate_graph_structure(const py::dict& graph) {
    if (!graph.contains("nodes")) {
        throw std::invalid_argument("Graph must contain 'nodes' key");
    }
    if (!graph.contains("edges")) {
        throw std::invalid_argument("Graph must contain 'edges' key");
    }

    try {
        py::list nodes = safe_dict_get_list(graph, "nodes");
        py::list edges = safe_dict_get_list(graph, "edges");

        for (py::handle node_obj : nodes) {
            if (!py::isinstance<py::dict>(node_obj)) {
                throw std::invalid_argument("Each node must be a dictionary");
            }
            py::dict node = py::reinterpret_borrow<py::dict>(node_obj);
            if (!node.contains("data")) {
                throw std::invalid_argument("Node must contain 'data' key");
            }
        }

        for (py::handle edge_obj : edges) {
            if (!py::isinstance<py::dict>(edge_obj)) {
                throw std::invalid_argument("Each edge must be a dictionary");
            }
            py::dict edge = py::reinterpret_borrow<py::dict>(edge_obj);
            if (!edge.contains("data")) {
                throw std::invalid_argument("Edge must contain 'data' key");
            }
        }
    } catch (const std::exception& e) {
        throw std::invalid_argument(std::string("Graph validation failed: ") + e.what());
    }
}


void validate_node_data(const py::dict& data, size_t index) {
    if (!data.contains("id")) {
        throw std::invalid_argument("Node at index " + std::to_string(index) + " missing required 'id' field");
    }
    
    try {
        py::str id = data["id"];
        if (py::str(id).cast<std::string>().empty()) {
            throw std::invalid_argument("Node at index " + std::to_string(index) + " has empty 'id'");
        }
    } catch (const py::type_error&) {
        throw std::invalid_argument("Node at index " + std::to_string(index) + " 'id' must be a string");
    }
}


void validate_edge_data(const py::dict& data, size_t index) {
    if (!data.contains("source")) {
        throw std::invalid_argument("Edge at index " + std::to_string(index) + " missing required 'source' field");
    }
    if (!data.contains("target")) {
        throw std::invalid_argument("Edge at index " + std::to_string(index) + " missing required 'target' field");
    }
    
    try {
        std::string source = py::str(data["source"]);
        std::string target = py::str(data["target"]);
        
        if (source.empty()) {
            throw std::invalid_argument("Edge at index " + std::to_string(index) + " has empty 'source'");
        }
        if (target.empty()) {
            throw std::invalid_argument("Edge at index " + std::to_string(index) + " has empty 'target'");
        }
    } catch (const py::type_error& e) {
        throw std::invalid_argument("Edge at index " + std::to_string(index) + " source/target must be strings: " + std::string(e.what()));
    }
}

py::dict make_edge_py(const std::string& source, const std::string& target, const std::string& label,
                      const std::string& edge_id) {
    py::dict edge_data;
    edge_data["id"] = next_id();
    edge_data["source"] = source;
    edge_data["target"] = target;
    edge_data["label"] = label;
    edge_data["edge_id"] = edge_id;
    py::dict edge;
    edge["data"] = edge_data;
    return edge;
}

std::vector<NodeData> parse_nodes(const py::dict& graph) {
    std::vector<NodeData> nodes;
    
    try {
        py::list raw_nodes = safe_dict_get_list(graph, "nodes");
        nodes.reserve(raw_nodes.size());
        
        size_t index = 0;
        for (py::handle obj : raw_nodes) {
            if (!py::isinstance<py::dict>(obj)) {
                throw std::invalid_argument("Node at index " + std::to_string(index) + " is not a dictionary");
            }
            
            py::dict node = py::reinterpret_borrow<py::dict>(obj);
            py::dict data = safe_dict_get_dict(node, "data");
            
            if (data.size() == 0) {
                throw std::invalid_argument("Node at index " + std::to_string(index) + " has empty data");
            }
            
            validate_node_data(data, index);
            
            NodeData item;
            item.id = safe_dict_get_string(data, "id");
            item.type = safe_dict_get_string(data, "type", "");
            item.name = safe_dict_get_string(data, "name", "");
            item.parent = safe_dict_get_string(data, "parent", "");
            item.data = py::dict(data);
            
            nodes.push_back(std::move(item));
            index++;
        }
    } catch (const std::exception& e) {
        throw std::invalid_argument("Failed to parse nodes: " + std::string(e.what()));
    }
    
    return nodes;
}

std::vector<EdgeData> parse_edges(const py::dict& graph) {
    std::vector<EdgeData> edges;
    
    try {
        py::list raw_edges = safe_dict_get_list(graph, "edges");
        edges.reserve(raw_edges.size());
        
        size_t index = 0;
        for (py::handle obj : raw_edges) {
            if (!py::isinstance<py::dict>(obj)) {
                throw std::invalid_argument("Edge at index " + std::to_string(index) + " is not a dictionary");
            }
            
            py::dict edge = py::reinterpret_borrow<py::dict>(obj);
            py::dict data = safe_dict_get_dict(edge, "data");
            
            if (data.size() == 0) {
                throw std::invalid_argument("Edge at index " + std::to_string(index) + " has empty data");
            }
            
            validate_edge_data(data, index);
            
            EdgeData item;
            item.source = safe_dict_get_string(data, "source");
            item.target = safe_dict_get_string(data, "target");
            item.label = safe_dict_get_string(data, "label", "");
            item.edge_id = safe_dict_get_string(data, "edge_id", "");
            
            edges.push_back(std::move(item));
            index++;
        }
    } catch (const std::exception& e) {
        throw std::invalid_argument("Failed to parse edges: " + std::string(e.what()));
    }
    
    return edges;
}

ParsedGraph parse_graph(const py::dict& graph) {
    try {
        validate_graph_structure(graph);
        return {parse_nodes(graph), parse_edges(graph)};
    } catch (const std::exception& e) {
        throw std::invalid_argument("Graph parsing failed: " + std::string(e.what()));
    }
}

py::dict build_py_dict(const ParsedGraph& pg) {
    py::list nodes;
    for (const auto& node : pg.nodes) {
        py::dict wrapped;
        wrapped["data"] = node.data;
        nodes.append(wrapped);
    }
    py::list edges;
    for (const auto& e : pg.edges) {
        edges.append(make_edge_py(e.source, e.target, e.label, e.edge_id));
    }
    py::dict out;
    out["nodes"] = nodes;
    out["edges"] = edges;
    return out;
}


CollapseResult collapse_nodes_internal(const ParsedGraph& pg) {
    const auto& nodes = pg.nodes;
    const auto& edges = pg.edges;

    std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>> inbound;
    std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>> outbound;
    std::unordered_map<std::string, NodeData> node_map;
    node_map.reserve(nodes.size());

    for (const NodeData& node : nodes) {
        inbound[node.id] = {};
        outbound[node.id] = {};
        node_map[node.id] = node;
    }

    for (const EdgeData& edge : edges) {
        outbound[edge.source].push_back({edge.target, edge.edge_id});
        inbound[edge.target].push_back({edge.source, edge.edge_id});
    }

    std::unordered_map<std::pair<std::vector<std::pair<std::string, std::string>>,
                                  std::vector<std::pair<std::string, std::string>>>,
                       std::vector<std::string>,
                       SignatureHash> signature_groups;

    for (const NodeData& node : nodes) {
        auto in_edges = inbound[node.id];
        auto out_edges = outbound[node.id];
        std::sort(in_edges.begin(), in_edges.end());
        std::sort(out_edges.begin(), out_edges.end());
        signature_groups[{in_edges, out_edges}].push_back(node.id);
    }

    CollapseResult result;
    auto& result_nodes = result.graph.nodes;
    auto& result_edges = result.graph.edges;
    auto& group_for_node = result.group_for_node;

    for (const auto& item : signature_groups) {
        const std::vector<std::string>& grouped_ids = item.second;
        const NodeData& first = node_map[grouped_ids.front()];
        std::string base_label = first.id;
        std::size_t space = base_label.find(' ');
        if (space != std::string::npos) {
            base_label = base_label.substr(0, space);
        }

        std::string grouped_id = next_id();
        std::string name =
            grouped_ids.size() == 1 ? first.name : std::to_string(grouped_ids.size()) + " " + base_label + " nodes";

        py::list members;
        for (const std::string& node_id : grouped_ids) {
            members.append(node_map[node_id].data);
            group_for_node[node_id] = grouped_id;
        }

        NodeData collapsed_node;
        collapsed_node.id = grouped_id;
        collapsed_node.type = base_label;
        collapsed_node.name = name;
        py::dict data;
        data["id"] = grouped_id;
        data["type"] = base_label;
        data["name"] = name;
        data["nodes"] = members;
        collapsed_node.data = data;
        result_nodes.push_back(std::move(collapsed_node));
    }

    std::unordered_set<std::string> seen;
    result_edges.reserve(edges.size());
    for (const EdgeData& edge : edges) {
        auto source_it = group_for_node.find(edge.source);
        auto target_it = group_for_node.find(edge.target);
        if (source_it == group_for_node.end() || target_it == group_for_node.end()) {
            continue;
        }
        const std::string& src = source_it->second;
        const std::string& tgt = target_it->second;
        if (src == tgt) {
            continue;
        }
        std::string key = src + "|" + tgt + "|" + edge.edge_id;
        if (seen.find(key) != seen.end()) {
            continue;
        }
        seen.insert(key);
        result_edges.push_back({src, tgt, edge.label, edge.edge_id});
    }

    return result;
}

ParsedGraph group_into_parents_internal(ParsedGraph pg) {
    auto& nodes = pg.nodes;
    auto& edges = pg.edges;

    std::unordered_set<std::string> node_ids;
    node_ids.reserve(nodes.size());
    for (const NodeData& node : nodes) {
        node_ids.insert(node.id);
    }

    std::unordered_map<std::string, std::unordered_map<std::string, std::set<std::string>>> out_map;
    std::unordered_map<std::string, std::unordered_map<std::string, std::set<std::string>>> in_map;
    for (const std::string& node_id : node_ids) {
        out_map[node_id] = {};
        in_map[node_id] = {};
    }

    for (const EdgeData& edge : edges) {
        out_map[edge.source][edge.edge_id].insert(edge.target);
        in_map[edge.target][edge.edge_id].insert(edge.source);
    }

    struct ParentInfo {
        std::string id;
        std::string node;
        std::string edge_id;
        std::string label;
        int count;
        bool is_source;
        std::vector<std::string> cached_nodes;
        std::unordered_set<std::string> cached_nodes_set;

        ParentInfo() = default;
        ParentInfo(const std::string& _id, const std::string& _node, const std::string& _edge_id,
                   const std::string& _label, int _count, bool _is_source,
                   const std::vector<std::string>& _nodes)
            : id(_id), node(_node), edge_id(_edge_id), label(_label), count(_count), is_source(_is_source),
              cached_nodes(_nodes) {
            for (const auto& n : _nodes) {
                cached_nodes_set.insert(n);
            }
        }
    };

    std::unordered_map<std::string, ParentInfo> parent_map;
    std::vector<std::pair<std::string, ParentInfo*>> parent_list;

    for (const std::string& node_id : node_ids) {
        for (const auto& out_item : out_map[node_id]) {
            const std::string& edge_id = out_item.first;
            const std::set<std::string>& neighbors = out_item.second;
            if (neighbors.size() < 2) {
                continue;
            }
            std::vector<std::string> values(neighbors.begin(), neighbors.end());
            std::string key = join(values, ",");
            if (parent_map.find(key) == parent_map.end()) {
                parent_map[key] = ParentInfo(next_id(), node_id, edge_id, extract_middle(edge_id),
                                             static_cast<int>(neighbors.size()), true, values);
                parent_list.push_back({key, &parent_map[key]});
            }
        }
        for (const auto& in_item : in_map[node_id]) {
            const std::string& edge_id = in_item.first;
            const std::set<std::string>& neighbors = in_item.second;
            if (neighbors.size() < 2) {
                continue;
            }
            std::vector<std::string> values(neighbors.begin(), neighbors.end());
            std::string key = join(values, ",");
            if (parent_map.find(key) == parent_map.end()) {
                parent_map[key] = ParentInfo(next_id(), node_id, edge_id, extract_middle(edge_id),
                                             static_cast<int>(neighbors.size()), false, values);
                parent_list.push_back({key, &parent_map[key]});
            }
        }
    }

    std::sort(parent_list.begin(), parent_list.end(),
              [](const auto& a, const auto& b) { return a.second->count < b.second->count; });

    std::unordered_set<std::string> invalid;
    for (std::size_t i = 0; i < parent_list.size(); ++i) {
        if (invalid.find(parent_list[i].first) != invalid.end()) {
            continue;
        }

        const ParentInfo& current = *parent_list[i].second;
        const std::unordered_set<std::string>& current_set = current.cached_nodes_set;

        for (std::size_t j = i + 1; j < parent_list.size(); ++j) {
            const ParentInfo& other = *parent_list[j].second;

            if (other.is_source != current.is_source) {
                continue;
            }

            bool overlap = false;
            for (const std::string& node : other.cached_nodes) {
                if (current_set.find(node) != current_set.end()) {
                    overlap = true;
                    break;
                }
            }

            if (overlap) {
                invalid.insert(parent_list[i].first);
                break;
            }
        }
    }

    for (const std::string& key : invalid) {
        parent_map.erase(key);
    }

    std::unordered_set<std::string> parents;
    std::unordered_map<std::string, int> grouped_node_count;
    std::unordered_map<std::string, std::vector<NodeData*>> grouped_nodes;

    for (auto& node : nodes) {
        std::string node_id = node.id;
        int node_count = 0;

        for (const auto& item : parent_map) {
            const ParentInfo& parent = item.second;
            if (parent.cached_nodes_set.count(node_id) && parent.count > node_count) {
                node.data["parent"] = parent.id;
                node.parent = parent.id;
                node_count = parent.count;
            }
        }

        if (!node.parent.empty()) {
            parents.insert(node.parent);
            grouped_node_count[node.parent] += 1;
            grouped_nodes[node.parent].push_back(&node);
        }
    }

    for (const auto& item : grouped_node_count) {
        if (item.second >= 2) {
            continue;
        }
        const std::string& parent_id = item.first;
        parents.erase(parent_id);
        for (NodeData* child : grouped_nodes[parent_id]) {
            child->data["parent"] = "";
            child->parent = "";
        }
    }

    for (const std::string& parent_id : parents) {
        NodeData parent_node;
        parent_node.id = parent_id;
        parent_node.type = "parent";
        parent_node.name = parent_id;
        py::dict data;
        data["id"] = parent_id;
        data["type"] = "parent";
        data["name"] = parent_id;
        parent_node.data = data;
        nodes.push_back(std::move(parent_node));
    }

    std::vector<EdgeData> new_edges;
    new_edges.reserve(edges.size());
    for (const EdgeData& edge : edges) {
        bool keep = true;
        for (const auto& item : parent_map) {
            const ParentInfo& parent = item.second;
            if (parents.find(parent.id) == parents.end()) {
                continue;
            }
            std::string edge_key = parent.is_source ? edge.target : edge.source;
            std::string parent_node = parent.is_source ? edge.source : edge.target;
            if (parent.cached_nodes_set.count(edge_key) &&
                parent.node == parent_node && parent.edge_id == edge.edge_id) {
                keep = false;
                break;
            }
        }
        if (keep) {
            new_edges.push_back(edge);
        }
    }

    for (const auto& item : parent_map) {
        const ParentInfo& parent = item.second;
        if (parents.find(parent.id) == parents.end()) {
            continue;
        }
        std::string source = parent.is_source ? parent.node : parent.id;
        std::string target = parent.is_source ? parent.id : parent.node;
        new_edges.push_back({source, target, parent.label, parent.edge_id});
    }

    pg.edges = std::move(new_edges);
    return pg;
}

}  

py::dict group_graph(const py::dict& graph) {
    try {
        if (!graph.contains("nodes") || !graph.contains("edges")) {
            throw std::invalid_argument("Graph must contain both 'nodes' and 'edges' keys");
        }
        
        auto pg = parse_graph(graph);
        
        if (pg.nodes.empty()) {
            py::dict result;
            result["nodes"] = py::list();
            result["edges"] = py::list();
            return result;
        }
                auto cr = collapse_nodes_internal(pg);
        // Group into parents – works on C++ data directly
        auto result = group_into_parents_internal(std::move(cr.graph));
        // Build back to py::dict ONCE
        return build_py_dict(result);
    } catch (const std::invalid_argument& e) {
        throw pybind11::value_error("Invalid input: " + std::string(e.what()));
    } catch (const std::exception& e) {
        throw std::runtime_error("group_graph failed: " + std::string(e.what()));
    }
}

py::dict group_node_only(const py::dict& graph, const py::dict& request) {
    try {
        py::dict node_map_by_label;
        py::dict out;
        out["nodes"] = py::list();
        out["edges"] = py::list();

        if (!request.contains("nodes")) {
            return out;
        }
        
        py::list request_nodes = safe_dict_get_list(request, "nodes");
        if (request_nodes.size() == 0) {
            return out;
        }
        
        for (size_t i = 0; i < request_nodes.size(); ++i) {
            py::handle obj = request_nodes[i];
            if (!py::isinstance<py::dict>(obj)) {
                throw std::invalid_argument("Request node at index " + std::to_string(i) + " is not a dictionary");
            }
            py::dict node = py::reinterpret_borrow<py::dict>(obj);
            if (!node.contains("type")) {
                throw std::invalid_argument("Request node at index " + std::to_string(i) + " missing 'type' field");
            }
            std::string type = safe_dict_get_string(node, "type");
            if (type.empty()) {
                throw std::invalid_argument("Request node at index " + std::to_string(i) + " has empty 'type'");
            }
            node_map_by_label[type.c_str()] = py::list();
        }

        validate_graph_structure(graph);
        py::list input_nodes = safe_dict_get_list(graph, "nodes");
        
        for (size_t i = 0; i < input_nodes.size(); ++i) {
            py::handle obj = input_nodes[i];
            if (!py::isinstance<py::dict>(obj)) {
                throw std::invalid_argument("Graph node at index " + std::to_string(i) + " is not a dictionary");
            }
            py::dict wrapped = py::reinterpret_borrow<py::dict>(obj);
            py::dict data = safe_dict_get_dict(wrapped, "data");
            if (data.size() == 0) {
                throw std::invalid_argument("Graph node at index " + std::to_string(i) + " has empty data");
            }
            std::string type = safe_dict_get_string(data, "type", "");
            if (node_map_by_label.contains(type.c_str())) {
                py::list values = py::reinterpret_borrow<py::list>(node_map_by_label[type.c_str()]);
                values.append(data);
            }
        }

        py::list output_nodes = py::reinterpret_borrow<py::list>(out["nodes"]);
        for (auto item : node_map_by_label) {
            py::str key = py::reinterpret_borrow<py::str>(item.first);
            std::string node_type = py::str(key);
            py::list values = py::reinterpret_borrow<py::list>(node_map_by_label[key]);
            py::dict data;
            data["id"] = next_id();
            data["type"] = node_type;
            data["name"] = std::to_string(values.size()) + " " + node_type + " nodes";
            data["nodes"] = values;
            py::dict wrapped;
            wrapped["data"] = data;
            output_nodes.append(wrapped);
        }
        return out;
    } catch (const std::invalid_argument& e) {
        throw pybind11::value_error("Invalid input: " + std::string(e.what()));
    } catch (const std::exception& e) {
        throw std::runtime_error("group_node_only failed: " + std::string(e.what()));
    }
}

py::dict break_grouping(const py::dict& graph) {
    try {
        validate_graph_structure(graph);
        std::vector<NodeData> nodes = parse_nodes(graph);
        std::vector<EdgeData> edges = parse_edges(graph);
        
        std::unordered_map<std::string, std::vector<std::string>> parent_edges;
        std::unordered_map<std::string, py::dict> initial_node_map;

        for (const NodeData& node : nodes) {
            initial_node_map[node.id] = node.data;
            if (node.type == "parent") {
                parent_edges[node.id] = {};
            }
        }

        for (const NodeData& node : nodes) {
            if (node.type == "protein" && !node.parent.empty() &&
                parent_edges.find(node.parent) != parent_edges.end()) {
                parent_edges[node.parent].push_back(node.id);
            }
        }

        std::vector<EdgeData> expanded_edges;
        for (const EdgeData& edge : edges) {
            auto src_parent = parent_edges.find(edge.source);
            auto tgt_parent = parent_edges.find(edge.target);
            if (src_parent != parent_edges.end()) {
                for (const std::string& child : src_parent->second) {
                    expanded_edges.push_back({child, edge.target, edge.label, edge.edge_id});
                }
            } else if (tgt_parent != parent_edges.end()) {
                for (const std::string& child : tgt_parent->second) {
                    expanded_edges.push_back({edge.source, child, edge.label, edge.edge_id});
                }
            } else {
                expanded_edges.push_back(edge);
            }
        }

        struct RawEdge {
            std::string source;
            std::string target;
            std::string label;
            std::string edge_id;
        };
        std::unordered_map<std::string, RawEdge> expanded_map;
        for (const EdgeData& edge : expanded_edges) {
            if (initial_node_map.find(edge.source) == initial_node_map.end() ||
                initial_node_map.find(edge.target) == initial_node_map.end()) {
                continue;
            }
            
            try {
                py::dict source = initial_node_map[edge.source];
                py::dict target = initial_node_map[edge.target];

                std::vector<std::string> source_nodes;
                std::vector<std::string> target_nodes;
                
                std::string source_type = safe_dict_get_string(source, "type", "");
                if (source_type != "parent" && source.contains("nodes")) {
                    try {
                        py::list source_list = safe_dict_get_list(source, "nodes");
                        for (size_t i = 0; i < source_list.size(); ++i) {
                            py::handle item = source_list[i];
                            if (py::isinstance<py::dict>(item)) {
                                py::dict node = py::reinterpret_borrow<py::dict>(item);
                                source_nodes.push_back(safe_dict_get_string(node, "id", ""));
                            }
                        }
                    } catch (const std::exception&) {
                    }
                }
                
                std::string target_type = safe_dict_get_string(target, "type", "");
                if (target_type != "parent" && target.contains("nodes")) {
                    try {
                        py::list target_list = safe_dict_get_list(target, "nodes");
                        for (size_t i = 0; i < target_list.size(); ++i) {
                            py::handle item = target_list[i];
                            if (py::isinstance<py::dict>(item)) {
                                py::dict node = py::reinterpret_borrow<py::dict>(item);
                                target_nodes.push_back(safe_dict_get_string(node, "id", ""));
                            }
                        }
                    } catch (const std::exception&) {
                        continue;
                    }
                }

                for (const std::string& source_node : source_nodes) {
                    for (const std::string& target_node : target_nodes) {
                        if (!source_node.empty() && !target_node.empty()) {
                            std::string key = source_node + "|" + edge.label + "|" + target_node + "|" + edge.edge_id;
                            expanded_map[key] = {source_node, target_node, edge.label, edge.edge_id};
                        }
                    }
                }
            } catch (const std::exception& e) {
                continue;
            }
        }

        py::list out_edges;
        for (const auto& item : expanded_map) {
            const RawEdge& edge = item.second;
            out_edges.append(make_edge_py(edge.source, edge.target, edge.label, edge.edge_id));
        }

        py::list out_nodes;
        for (const NodeData& node : nodes) {
            if (node.type == "parent") {
                continue;
            }
            if (!node.data.contains("nodes")) {
                continue;
            }
            
            try {
                py::list node_list = safe_dict_get_list(node.data, "nodes");
                for (py::handle item : node_list) {
                    if (py::isinstance<py::dict>(item)) {
                        py::dict wrapped;
                        wrapped["data"] = py::reinterpret_borrow<py::dict>(item);
                        out_nodes.append(wrapped);
                    }
                }
            } catch (const std::exception&) {
                // Skip malformed nodes
                continue;
            }
        }

        py::dict out;
        out["nodes"] = out_nodes;
        out["edges"] = out_edges;
        return out;
    } catch (const std::invalid_argument& e) {
        throw pybind11::value_error("Invalid input: " + std::string(e.what()));
    } catch (const std::exception& e) {
        throw std::runtime_error("break_grouping failed: " + std::string(e.what()));
    }
}

} 
