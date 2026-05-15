#pragma once

#include <pybind11/pybind11.h>

namespace graph_native {

namespace py = pybind11;

py::dict group_graph(const py::dict& graph);
py::dict group_node_only(const py::dict& graph, const py::dict& request);
py::dict break_grouping(const py::dict& graph);

}  
