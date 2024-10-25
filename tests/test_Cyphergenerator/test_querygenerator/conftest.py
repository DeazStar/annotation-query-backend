import pytest

#query with only one node and no predicate
query_withoutpredicate = {
        "node": {
            "nodes": [
                {
                    "node_id": "n1",
                    "id": "",
                    "type": "gene",
                    "properties": {} 
                }]
            },
        "expected_output": ['MATCH (n_n1:gene) RETURN n_n1']
         }

#query with only one node and a node id
query_one_node_noid = {
          "node": {
            "nodes": [{
                "node_id": "n1",
                "id": "",
                "type": "gene",
                "properties": {} 
              }],
		        "predicates": []
          },
          "expected_output": ['MATCH (n_n1:gene) RETURN n_n1']
        }

query_one_node_with_id = {
          "node": {
            "nodes": [{
                "node_id": "n1",
                "id": "testcase",
                "type": "gene",
                "properties": {} 
              }],
		        "predicates": []
          },
          "expected_output": ['MATCH (n_n1:gene {"id: testcase"}) RETURN n_n1']
        }

query_one_node_with_id_properties = {
          "node": {
            "nodes": [{
                "node_id": "n1",
                "id": "testcase",
                "type": "gene",
                "properties": {
                  "gene_type": "protein_coding"
                }
              }],
		        "predicates": []
          },
          "expected_output": ["MATCH (n_n1:gene {id: 'testcase'}) RETURN n_n1"]
        }

query_one_node_noid_with_oneproperty = {
          "node": {
            "nodes": [{
                "node_id": "n1",
                "id": "",
                "type": "gene",
                "properties": {
                  "gene_type": "protein_coding"
                }
              }],
		        "predicates": []
          },
          "expected_output": ["MATCH (n_n1:gene) WHERE n_n1.gene_type =~ '(?i)protein_coding' RETURN n_n1"]
        }

query_one_node_noid_with_properties = {
          "node": {
            "nodes": [{
                "node_id": "n1",
                "id": "",
                "type": "gene",
                "properties": {
                  "gene_type": "protein_coding",
                  "chr": "chr1"
                }
              }],
		        "predicates": []
          },
          "expected_output": ["MATCH (n_n1:gene) WHERE n_n1.gene_type =~ '(?i)protein_coding' AND n_n1.chr =~ '(?i)chr1' RETURN n_n1"]
        }

query_two_node_noid_noproperties = {
          "node": {
            "nodes": [{
                "node_id": "n1",
                "id": "",
                "type": "gene",
                "properties": {}
                },
                {
                "node_id": "n2",
                "id": "",
                "type": "gene",
                "properties": {}
                }
              ],
		        "predicates": []
          },
          "expected_output":  ['MATCH (n_n1:gene), (n_n2:gene) RETURN n_n1, n_n2', 'MATCH (n_n1:gene), (n_n2:gene) RETURN n_n2, n_n1']
        }

query_two_node_noid_with_properties = {
          "node": {
            "nodes": [{
                "node_id": "n1",
                "id": "",
                "type": "gene",
                "properties": {
                  "gene_type": "protein_coding"
                  }
                },
                {
                "node_id": "n2",
                "id": "",
                "type": "gene",
                "properties": {
                  "chr": "chr1"
                  }
                }
              ],
		        "predicates": []
          },
          "expected_output": ["MATCH (n_n1:gene), (n_n2:gene) WHERE n_n1.gene_type =~ '(?i)protein_coding' AND n_n2.chr =~ '(?i)chr1' RETURN n_n1, n_n2", "MATCH (n_n1:gene), (n_n2:gene) WHERE n_n1.gene_type =~ '(?i)protein_coding' AND n_n2.chr =~ 'chr1' RETURN n_n2, n_n1"]
        }

query_two_node_id_with_properties = {
          "node": {
            "nodes": [{
                "node_id": "n1",
                "id": "testcase",
                "type": "gene",
                "properties": {
                  "gene_type": "protein_coding"
                  }
                },
                {
                "node_id": "n2",
                "id": "",
                "type": "gene",
                "properties": {
                  "chr": "chr1"
                  }
                }
              ],
		        "predicates": []
          },
          "expected_output": ["MATCH (n_n1:gene {id: 'testcase'}), (n_n2:gene) WHERE n_n2.chr =~ '(?i)chr1' RETURN n_n1, n_n2", "MATCH (n_n1:gene {id: 'testcase'}), (n_n2:gene) WHERE n_n2.chr =~ '(?i)chr1' RETURN n_n2, n_n1"]
        }

query_two_node_with_predicate = {
          "node": {
            "nodes": [{
                "node_id": "n1",
                "id": "testcase",
                "type": "gene",
                "properties": {
                  "gene_type": "protein_coding"
                  }
                },
                {
                "node_id": "n2",
                "id": "",
                "type": "transcript",
                "properties": {}
                }
              ],
		        "predicates": [{
              "type": "transcribed to",
              "source": "n1",
              "target": "n2"
              }]
          },
          "expected_output":  ["MATCH (n1:gene {id: 'testcase'}), (n1)-[r0:transcribed_to]->(n2:transcript) RETURN r0, n1, n2", "MATCH (n1:gene {id: 'testcase'}), (n1)-[r0:transcribed_to]->(n2:transcript) RETURN r0, n2, n1"]
        }

query_three_node_with_predicate = {
      "node": {
        "nodes": [
          {
            "node_id": "n1",
            "id": "",
            "type": "gene",
            "properties": {
              "gene_type": "protein_coding"
            }
          },
          {
            "node_id": "n2",
            "id": "",
            "type": "transcript",
            "properties": {}
          },
          {
            "node_id": "n3",
            "id": "",
            "type": "protein",
            "properties": {
              "protein_name": "ANKE1"
            }
          }
        ],
        "predicates": [
          {
            "type": "transcribed to",
            "source": "n1",
            "target": "n2"
          }
      ]
      },
    "expected_output": ["MATCH (n1:gene), (n1)-[r0:transcribed_to]->(n2:transcript) WHERE n1.gene_type =~ '(?i)protein_coding' RETURN r0, n1, n2 , null AS n_n3 UNION MATCH (n_n3:protein) WHERE n_n3.protein_name =~ '(?i)ANKE1' RETURN  n_n3 , null AS r0, null AS n1, null AS n2",
                        "MATCH (n1:gene), (n1)-[r0:transcribed_to]->(n2:transcript) WHERE n1.gene_type =~ '(?i)protein_coding' RETURN r0, n2, n1 , null AS n_n3 UNION MATCH (n_n3:protein) WHERE n_n3.protein_name =~ '(?i)ANKE1' RETURN  n_n3 , null AS r0, null AS n2, null AS n1"]
    }

query_json = {
  "node": {
    "nodes": [
      {
        "node_id": "n1",
        "id": "",
        "type": "gene",
        "properties": {
          "gene_type": "protein_coding"
        }
      },
      {
        "node_id": "n2",
        "id": "",
        "type": "transcript",
        "properties": {}
      },
      {
	"node_id": "n3",
        "id": "",
        "type": "protein",
        "properties": {
          "protein_name": "ANKE1"
        }
      }
    ],
    "predicates": [
      {
        "type": "transcribed to",
        "source": "n1",
        "target": "n2"
      }
   ]
  },
  "expected_output": ["MATCH (n1:gene), (n1)-[r0:transcribed_to]->(n2:transcript) WHERE n1.gene_type =~ '(?i)protein_coding' RETURN r0, n2, n1 , null AS n_n3 UNION MATCH (n_n3:protein) WHERE n_n3.protein_name =~ '(?i)ANKE1' RETURN  n_n3 , null AS r0, null AS n2, null AS n1", 
                      "MATCH (n1:gene), (n1)-[r0:transcribed_to]->(n2:transcript) WHERE n1.gene_type =~ '(?i)protein_coding' RETURN r0, n1, n2 , null AS n_n3 UNION MATCH (n_n3:protein) WHERE n_n3.protein_name =~ '(?i)ANKE1' RETURN  n_n3 , null AS r0, null AS n1, null AS n2"]
}

@pytest.fixture(params=[
  query_one_node_noid_with_properties,
  query_one_node_noid_with_oneproperty,
  query_one_node_with_id_properties,
  query_one_node_noid,
  query_withoutpredicate
])
def one_query_list(request):
    return request.param

@pytest.fixture(params=[
  query_json,
  query_three_node_with_predicate,
  query_two_node_with_predicate,
  query_two_node_id_with_properties,
  query_two_node_noid_with_properties,
  query_two_node_noid_noproperties,
])
def multiple_query_list(request):
    return request.param
