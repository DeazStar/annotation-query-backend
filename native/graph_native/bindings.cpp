#include <pybind11/pybind11.h>

#include "graph_ops.hpp"

namespace py = pybind11;

PYBIND11_MODULE(graph_native, m) {
    m.doc() = "Native graph grouping operations";
    m.def("group_graph", &graph_native::group_graph, "Group graph operations");
    m.def("group_node_only", &graph_native::group_node_only, "Group nodes for node-only graph");
    m.def("break_grouping", &graph_native::break_grouping, "Break grouped graph into original nodes");
}
