running_processes={}
import asyncio 
import socketio
import datetime
from app.services.cypher_generator import CypherQueryGenerator
from app import app, databases, schema_manager, db_instance 
llm = app.config['llm_handler']
storage_service = app.config['storage_service']
 
async def process_query_tasks(result, annotation_id, properties, room):
    try:
        if annotation_id in running_processes and running_processes[annotation_id].get('cancelled', False):
                task=running_processes['task']
                if task.cancelled():
                    raise asyncio.CancelledError
        # Assuming 'result' is your list of records
        if len(result) > 1:  # Check if there are at least 2 records
            record = result[1]  # Get the second record (index 1)
            print("record",record[0])
            node_count_by_label= record[0].get('nodes_count_by_label')
            edge_count_by_type=record[0].get('edges_count_by_type')
            
  
        node_and_edge_count = result[1] if len(result) > 1 else []
        matched_result = result[0]
        print("step -1")
        if annotation_id in running_processes and running_processes[annotation_id].get('cancelled', False):
                task=running_processes['task']
                if task.cancelled():
                    raise asyncio.CancelledError
        
        graph=asyncio.create_task(generate_graph(matched_result, properties, room)),
        node_count_by_label, edge_count_by_label=asyncio.create_task(count_by_label_function(node_count_by_label,edge_count_by_type, annotation_id, room)),
        nodes,edges=asyncio.create_task(count_nodes_and_edges(node_and_edge_count, annotation_id, room))
        

        if annotation_id in running_processes and running_processes[annotation_id].get('cancelled', False):
                task=running_processes['task']
                if task.cancelled():
                    raise asyncio.CancelledError
        
 
        return graph, node_count_by_label, edge_count_by_label,nodes,edges

    except asyncio.CancelledError:
         
        print(f"Task with annotation_id {annotation_id} has been cancelled")
       
        return None, None, None
    except Exception as e:
        socketio.emit('update_event', {"error": f"Error in process_query_tasks: {str(e)}"}, room=room)
        return None, None, None
    
 



async def count_nodes_and_edges(node_and_edge_count, annotation_id, room):
    try:
        print("node count and edges")
        node_count, edge_count = CypherQueryGenerator.count_node_and_edges_function(node_and_edge_count)
        print ("node_count",node_count)
        print("edge_count",edge_count)


        update_annotation = {
            "node_count": node_count,
            "edge_count": edge_count,
            "updated_at": datetime.datetime.now()
        }
        storage_service.update(annotation_id, update_annotation)
        
        # print("update_event", {"status": "pending", "node_count": node_count, "edge_count": edge_count}, room=room)
        socketio.emit("update_event", {"status": "pending", "node_count": node_count, "edge_count": edge_count}, room=room)
        print("node count by label function ")
        task_tracker["node_count"] = True
        task_tracker["edge_count"] = True
        print("count_nodes_and_edges task completed")
        return node_count, edge_count
    except Exception as e:
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        raise e

async def count_by_label_function(node_count_by_label,edge_count_by_type, annotation_id, room):
    
    try:
         
         
        update_annotation = {
            "node_count_by_label": node_count_by_label,
            "edge_count_by_label": edge_count_by_type,
            "updated_at": datetime.datetime.now()
        }
        storage_service.update(annotation_id, update_annotation)

        print("node&&&&&&&&&&&&&&&&",node_count_by_label,"edges",edge_count_by_type)
        socketio.emit("update_event", {"node_count_by_label": node_count_by_label, "edge_count_by_label": edge_count_by_type}, room=room)
        print("node count by label function ")
        task_tracker["node_count_by_label"] = True
        task_tracker["edge_count_by_label"] = True
        return node_count_by_label, edge_count_by_type
    except Exception as e:
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        raise e

async def generate_graph(requests, properties, room):
    print("generate grpah function ")
    try:
        request_data = CypherQueryGenerator.graph_function(requests, properties)
        
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        if isinstance(request_data, tuple):
            # Convert tuple to dictionary if necessary
            request_data = {"nodes": request_data[0], "edges": request_data[1]}
        # print('update_event', {"graph": True}, room=room)
        socketio.emit('update_event', {"graph": True}, room=room)
        task_tracker["graph"] = True
        return request_data
    except Exception as asyncio.CancelledError:
        print("asyncio here")
    except Exception as e:
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        raise e

async def summary(graph, requests, node_count_by_label, edge_count_by_label, annotation_id, room):
    try:
        print("summary ")
        if annotation_id in running_processes and running_processes[annotation_id].get('cancelled', False):
            task = running_processes['task']
            if task.cancelled():
                raise asyncio.CancelledError
        
        summary = llm.generate_summary(graph, requests, node_count_by_label, edge_count_by_label) or 'Graph too big, could not summarize'
        summary = "summary of the graph is here"
        
        updated_annotation = {
            "summary": summary,
            "updated_at": datetime.datetime.now()
        }
        storage_service.update(annotation_id, updated_annotation)
        
        return summary
    
    except asyncio.CancelledError:
        print("Summary task has been cancelled")
        socketio.emit('update_event', {"status": "cancelled", "message": "Summary task has been cancelled"}, room=room)
        raise  # Re-raise the exception to propagate the cancellation
    except Exception as e:
        print(f"Exception occurred in summary: {e}")
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        raise