from setuptools import Extension, setup

import pybind11


ext_modules = [
    Extension(
        "graph_native",
        [
            "native/graph_native/bindings.cpp",
            "native/graph_native/graph_ops.cpp",
        ],
        include_dirs=[pybind11.get_include()],
        language="c++",
        extra_compile_args=["-O3", "-std=c++17"],
    )
]


setup(
    name="annotation-query-backend-native",
    version="0.1.0",
    description="Native graph operations for annotation query backend",
    ext_modules=ext_modules,
)
