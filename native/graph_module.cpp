#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "graph_core.hpp"

using namespace std;


namespace py = pybind11;

Graph py_to_graph(const py::dict& obj) {
    Graph g;
    if (obj.contains("nodes")) {
        py::list nodes = obj["nodes"].cast<py::list>();
        for (auto item : nodes) {
            py::dict n_dict = item.cast<py::dict>();
            Node n;
            py::dict data = n_dict.contains("data") ? n_dict["data"].cast<py::dict>() : n_dict;
            n.id = data["id"].cast<string>();
            for (auto const& [k, v] : data) {
                n.attrs[k.cast<string>()] = v.cast<py::object>();
            }
            g.nodes.push_back(n);
        }
    }
    if (obj.contains("edges")) {
        py::list edges = obj["edges"].cast<py::list>();
        for (auto item : edges) {
            py::dict e_dict = item.cast<py::dict>();
            Edge e;
            py::dict data = e_dict.contains("data") ? e_dict["data"].cast<py::dict>() : e_dict;
            e.id = data.contains("id") ? data["id"].cast<string>() : generate_nanoid();
            e.source = data["source"].cast<string>();
            e.target = data["target"].cast<string>();
            e.edge_id = data.contains("edge_id") ? data["edge_id"].cast<string>() : "";
            e.label = data.contains("label") ? data["label"].cast<string>() : "";
            for (auto const& [k, v] : data) {
                e.attrs[k.cast<string>()] = v.cast<py::object>();
            }
            g.edges.push_back(e);
        }
    }
    return g;
}

py::dict graph_to_py_wrapped(const Graph& g) {
    return convert_to_graph_json(g, true);
}

PYBIND11_MODULE(graph_native, m) {
    m.def("group_node_only", [](py::dict g, py::dict r) {
        return graph_to_py_wrapped(group_node_only(py_to_graph(g), r));
    });
    
    m.def("get_node_to_connections_map", [](py::dict g) {
        auto [mapping, nodes] = get_node_to_connections_map(py_to_graph(g));
        return make_pair(mapping, nodes);
    });
    
    m.def("collapse_nodes", [](py::dict g) {
        return graph_to_py_wrapped(collapse_nodes(py_to_graph(g)));
    });
    
    m.def("collapse_node_nx", [](py::dict g) {
        return graph_to_py_wrapped(collapse_node_nx(py_to_graph(g)));
    });
    
    m.def("convert_to_graph_json", [](py::dict g, bool allow_data) {
        return convert_to_graph_json(py_to_graph(g), allow_data);
    });
    
    m.def("group_into_parents", [](py::dict g) {
        return graph_to_py_wrapped(group_into_parents(py_to_graph(g)));
    });
    
    m.def("group_graph", [](py::dict g) {
        return graph_to_py_wrapped(group_graph(py_to_graph(g)));
    });
    
    m.def("break_grouping", [](py::dict g) {
        return graph_to_py_wrapped(break_grouping(py_to_graph(g)));
    });
    
    m.def("collapse_node_nx_location", [](py::dict g) {
        return graph_to_py_wrapped(collapse_node_nx_location(py_to_graph(g)));
    });
    
    m.def("build_graph_nx", [](py::dict g) {
        // Since we just build internal representation, and wrapper uses it for other things,
        // we just return the graph dict for now. Wrapper can convert to nx if needed.
        return g; 
    });
    
    m.def("build_subgraph_nx", [](py::dict g) {
        auto components = build_subgraph_nx(py_to_graph(g));
        py::list res;
        for (const auto& cg : components) res.append(graph_to_py_wrapped(cg));
        return res;
    });
}
