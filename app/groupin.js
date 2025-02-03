import { nanoid } from 'nanoid';

const groupGraph = (resultGraph, request) => {
    // we can modify the minimum number of duplicate edges for which we group nodes together
    const MINIMUM_EDGES_TO_COLLAPSE = 2;

    // get all the unique edge types specified in the query
    const edgeTypes = Array.from(
      new Set(request.edges.map((e) => e.data.edgeType)),
    );
    console.log("edge types",edgeTypes)
    // For each edge type in the query, try to group it according to source and then according to target and then compare which grouping works best.
    let edgeGroupings = edgeTypes.map((type) => {
      // find all edges of that type
      const edgesOfType = resultGraph.edges.filter(
        (e) => e.data.label === type,
      );
      console.log("edgesOfType",edgesOfType)
      // for each type, try to group with source
      const sourceGroups = Object.groupBy(
        edgesOfType,
        (e) => e.data.source,
      );
      console.log("sourceof groups",sourceGroups)
      // for each type, try to group with target
      const targetGroups = Object.groupBy(
        edgesOfType,
        (e) => e.data.target,
      );
      console.log(targetGroups)
      // compare which grouping to use. we prefer the one with fewer groups.
      const groupedBy =
        Object.keys(sourceGroups).length > Object.keys(targetGroups).length
          ? "target"
          : "source";
      console.log(groupedBy)
      // return the best grouping for this edge type
      return {
        count: edgesOfType.length,
        edgeType: type,
        groupedBy,
        groups: groupedBy === "source" ? sourceGroups : targetGroups,
      };
    });

    // the result of the optimization depends on which edge types we consider first when grouping nodes. We want to start from edges types that could remove most complexity (most number of edges in this case) from the graph.
    edgeGroupings = edgeGroupings.sort(
      (a, b) =>
        b.count -
        Object.keys(b.groups).length -
        (a.count - Object.keys(a.groups).length),
    );

    // for each edge group, we create a parent edge that holds nodes with the common edge, we add its id as 'parent' property in the nodes, we create a new edge of similar type that connects the newly created parent node instead of individual nodes, we remove the individual edges from the graph.
    const newGraph = { ...resultGraph };
    edgeGroupings.map((grouping) => {
      // we should sort the groups for a specific edge type, so that the ones with the most number of
      // edges are taken care of first.
      const sortedGroups = Object.keys(grouping.groups).sort(
        (a, b) => grouping.groups[b].length - grouping.groups[a].length,
      );

      sortedGroups.map((key) => {
        // get the duplicated edges
        const edges = grouping.groups[key];
        // ignore if there are too few edges to group
        if (edges.length < MINIMUM_EDGES_TO_COLLAPSE) return;
        // get the IDs of the nodes to be grouped
        const childNodeIDs = edges.map((e) =>
          grouping.groupedBy === "target" ? e.data.source : e.data.target,
        );
        // make sure none ose child nodes have a parent that is already specified for them. If they do have parent properties, it means they have already been grouped for a different edge type and we should skip them.
        const childNodes = newGraph.nodes.filter((n) =>
          childNodeIDs.includes(n.data.id),
        );
        const parentsOfChildNodes = childNodes.map((n) => {n.data.parent});
        const uniqueParents = Array.from(new Set(parentsOfChildNodes));
        // the nodes have different parents, so we can not group them together.
        if (uniqueParents.length > 1) return;
        // the nodes have a common parent. So we can create a new edge that points to
		// their parent rather than to individual nodes. but the parent might have other 
		//additional child nodes so we need to make sure the parent only contains the same nodes.
		const allChildNodesOfParent = newGraph.nodes.filter((n) => {
			console.log(`Node ID: ${n.data.id}, Parent: ${n.data.parent}`);
			return n.data.parent === uniqueParents[0];
		  });		  
        // if they all havere are no other nodes outside this group the the same parent and that have the same parent, 
		//we can draw an edge to the existing parent.
        if (
          uniqueParents[0] &&
          childNodes.length === allChildNodesOfParent.length
        ) {
          return addNewEdge(uniqueParents[0]);
        }
        // create the parent node
        const parentId = "n" + nanoid().replaceAll("-", "");
        const parent = {
          data: { id: parentId, type: "parent", parent: uniqueParents[0] },
        };
        // add the parent node to the graph and add "parent" propery to the child nodes
        newGraph.nodes = [
          parent,
          ...newGraph.nodes.map((n) => {
            if (childNodeIDs.includes(n.data.id))
              return { ...n, data: { ...n.data, parent: parentId } };
            return n;
          }),
        ];
        addNewEdge(parentId);
        function addNewEdge(parentId) {
          // add a new edge of the same type that points to the group
          const newEdgeId = "e" + nanoid().replaceAll("-", "");
          const newEdge = {
            data: {
              ...edges[0].data,
              id: newEdgeId,
              [grouping.groupedBy === "source" ? "target" : "source"]: parentId,
            },
          };
          newGraph.edges = [
            newEdge,
            ...newGraph.edges.filter((e) => {
              return !edges.find(
                (a) =>
                  a.data.label === e.data.label &&
                  a.data.target === e.data.target &&
                  a.data.source === e.data.source,
              );
            }),
          ];
        }
      });
    });
    return newGraph;
  }
  
const oldGraph = {
	"nodes": [
		{
			"data": {
				"id": "transcript enst00000456328",
				"type": "transcript",
				"name": "DDX11L2-202"
			}
		},
		{
			"data": {
				"id": "gene ensg00000290825",
				"type": "gene",
				"name": "DDX11L2"
			}
		},
		{
			"data": {
				"id": "exon ense00002312635",
				"type": "exon",
				"name": "exon ense00002312635"
			}
		},
		{
			"data": {
				"id": "exon ense00002234944",
				"type": "exon",
				"name": "exon ense00002234944"
			}
		},
		{
			"data": {
				"id": "exon ense00003582793",
				"type": "exon",
				"name": "exon ense00003582793"
			}
		},
		{
			"data": {
				"id": "transcript enst00000384476",
				"type": "transcript",
				"name": "RNVU1-15-201"
			}
		},
		{
			"data": {
				"id": "gene ensg00000207205",
				"type": "gene",
				"name": "RNVU1-15"
			}
		},
		{
			"data": {
				"id": "exon ense00001808588",
				"type": "exon",
				"name": "exon ense00001808588"
			}
		},
		{
			"data": {
				"id": "transcript enst00000364938",
				"type": "transcript",
				"name": "SNORA73A-201"
			}
		},
		{
			"data": {
				"id": "gene ensg00000274266",
				"type": "gene",
				"name": "SNORA73A"
			}
		},
		{
			"data": {
				"id": "exon ense00001439701",
				"type": "exon",
				"name": "exon ense00001439701"
			}
		},
		{
			"data": {
				"id": "transcript enst00000426952",
				"type": "transcript",
				"name": "HNRNPCP9-201"
			}
		},
		{
			"data": {
				"id": "gene ensg00000232048",
				"type": "gene",
				"name": "HNRNPCP9"
			}
		},
		{
			"data": {
				"id": "exon ense00001800064",
				"type": "exon",
				"name": "exon ense00001800064"
			}
		},
		{
			"data": {
				"id": "transcript enst00000450305",
				"type": "transcript",
				"name": "DDX11L1-201"
			}
		},
		{
			"data": {
				"id": "gene ensg00000223972",
				"type": "gene",
				"name": "DDX11L1"
			}
		},
		{
			"data": {
				"id": "exon ense00001863096",
				"type": "exon",
				"name": "exon ense00001863096"
			}
		},
		{
			"data": {
				"id": "exon ense00001758273",
				"type": "exon",
				"name": "exon ense00001758273"
			}
		},
		{
			"data": {
				"id": "exon ense00001671638",
				"type": "exon",
				"name": "exon ense00001671638"
			}
		},
		{
			"data": {
				"id": "exon ense00001948541",
				"type": "exon",
				"name": "exon ense00001948541"
			}
		}
	],
	"edges": [
		{
			"data": {
				"edge_id": "transcript_transcribed_from_gene",
				"label": "transcribed_from",
				"source": "transcript enst00000456328",
				"target": "gene ensg00000290825"
			}
		},
		{
			"data": {
				"edge_id": "transcript_includes_exon",
				"label": "includes",
				"source": "transcript enst00000456328",
				"target": "exon ense00002312635"
			}
		},
		{
			"data": {
				"edge_id": "transcript_includes_exon",
				"label": "includes",
				"source": "transcript enst00000456328",
				"target": "exon ense00002234944"
			}
		},
		{
			"data": {
				"edge_id": "transcript_includes_exon",
				"label": "includes",
				"source": "transcript enst00000456328",
				"target": "exon ense00003582793"
			}
		},
		{
			"data": {
				"edge_id": "transcript_transcribed_from_gene",
				"label": "transcribed_from",
				"source": "transcript enst00000384476",
				"target": "gene ensg00000207205"
			}
		},
		{
			"data": {
				"edge_id": "transcript_includes_exon",
				"label": "includes",
				"source": "transcript enst00000384476",
				"target": "exon ense00001808588"
			}
		},
		{
			"data": {
				"edge_id": "transcript_transcribed_from_gene",
				"label": "transcribed_from",
				"source": "transcript enst00000364938",
				"target": "gene ensg00000274266"
			}
		},
		{
			"data": {
				"edge_id": "transcript_includes_exon",
				"label": "includes",
				"source": "transcript enst00000364938",
				"target": "exon ense00001439701"
			}
		},
		{
			"data": {
				"edge_id": "transcript_transcribed_from_gene",
				"label": "transcribed_from",
				"source": "transcript enst00000426952",
				"target": "gene ensg00000232048"
			}
		},
		{
			"data": {
				"edge_id": "transcript_includes_exon",
				"label": "includes",
				"source": "transcript enst00000426952",
				"target": "exon ense00001800064"
			}
		},
		{
			"data": {
				"edge_id": "transcript_transcribed_from_gene",
				"label": "transcribed_from",
				"source": "transcript enst00000450305",
				"target": "gene ensg00000223972"
			}
		},
		{
			"data": {
				"edge_id": "transcript_includes_exon",
				"label": "includes",
				"source": "transcript enst00000450305",
				"target": "exon ense00001863096"
			}
		},
		{
			"data": {
				"edge_id": "transcript_includes_exon",
				"label": "includes",
				"source": "transcript enst00000450305",
				"target": "exon ense00001758273"
			}
		},
		{
			"data": {
				"edge_id": "transcript_includes_exon",
				"label": "includes",
				"source": "transcript enst00000450305",
				"target": "exon ense00001671638"
			}
		},
		{
			"data": {
				"edge_id": "transcript_includes_exon",
				"label": "includes",
				"source": "transcript enst00000450305",
				"target": "exon ense00001948541"
			}
		}
	],
	"node_count": 1113487,
	"edge_count": 1902311,
	"node_count_by_label": [
		{
			"count": 252835,
			"label": "transcript"
		},
		{
			"count": 62700,
			"label": "gene"
		},
		{
			"count": 797952,
			"label": "exon"
		}
	],
	"edge_count_by_label": [
		{
			"count": 252835,
			"relationship_type": "transcribed_from"
		},
		{
			"count": 1649476,
			"relationship_type": "includes"
		}
	],
	"title": "Relationships between transcripts, genes, and exons",
	"summary": "**Key Trends and Relationships:**\n\nThe graph data reveals a hierarchical relationship between genes, exons, and transcripts. Genes are transcribed into transcripts, which in turn include exons. This hierarchical structure is essential for understanding the flow of genetic information from DNA to RNA to protein.\n\n**Important Metrics:**\n\nThe graph contains 5 source nodes (genes), 10 target nodes (exons), and 11 edges (relationships). This relatively small size allows for easy visualization and analysis of the data.\n\n**Central Nodes:**\n\nThere are no central nodes in the graph, as all genes have only one transcript and each transcript includes only a few exons. This suggests that the genes in this dataset are not highly interconnected and may function independently.\n\n**Notable Structures:**\n\nThe graph does not exhibit any notable structures such as chains, hubs, or clusters. This is likely due to the small size and simple structure of the data.\n\n**Specific Characteristics:**\n\nThe data does not provide information about alternative splicing or regulatory mechanisms. However, the hierarchical structure of the graph suggests that alternative splicing may occur, as different transcripts can include different combinations of exons.\n\n**Notable Relationships:**\n\nThere are no notable relationships between nodes with a higher number of associated related nodes or complex processes. This is likely due to the small size and simple structure of the data.",
	"annotation_id": "67823527ae281b89a61fa243",
	"created_at": "2025-01-11T12:08:55.676000",
	"updated_at": "2025-01-11T12:08:55.676000"
}

const request = {
  "requests": {
    "nodes": [
      {
        "data": {
          "node_id": "n1",
          "id": "",
          "type": "transcript",
          "properties": {}
        }
      },
      {
        "data": {
          "node_id": "n2",
          "id": "",
          "type": "gene",
          "properties": {}
        }
      },
      {
        "data": {
          "node_id": "n3",
          "id": "",
          "type": "exon",
          "properties": {}
        }
      }
    ],
    "edges": [
      {
        "data": {
          "predicate_id": "p1",
          "edgeType": "transcribed from",
          "source": "n1",
          "target": "n2"
        }
      },
      {
        "data": {
          "predicate_id": "p2",
          "edgeType": "includes",
          "source": "n1",
          "target": "n3"
        }
      }
    ]
  }
};

  
let Ggraph = groupGraph(oldGraph, request.requests)
console.dir(Ggraph, { depth: null });
  
  