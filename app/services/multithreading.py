running_processes={}
import threading
import socketio
import datetime
from app.services.cypher_generator import CypherQueryGenerator
from app import app, databases, schema_manager, db_instance 
llm = app.config['llm_handler']
storage_service = app.config['storage_service']
 
exit_event=threading.Event()
tasks={}

class StopableThread(threading.Thread):
    def __init__(self,*args,**kwargs):
        super(StopableThread,self).__init__(*args,**kwargs)
        self._stop_event=threading.Event()
    def stop(self):
        self._stop_event.set()
    def stopped(self):
        return self._stop_event.is_set()

#TODO LAZY LOADING NEO4J RESULT 
def run_query(self,query_code,stop_event):
    try:
        with self.driver.session() as session:
            result = session.execute_read(lambda tx: tx.run(query_code))
            
             
            for record in result:
                if stop_event.is_set() or exit_event.is_set():
                    break
                yield record

    except Exception as e:
        yield {"error": "run_query", "error_value": str(e)}

def start_task(task_id):
    if task_id in tasks:
        return f'Task {task_id} is alredy running'
    stop_event=threading.event()
    task_thread=threading.Thread(target=run_query,args=(stop_event,))
    tasks[task_id]=(task_thread,stop_event)
    task_thread.start()
    return f"task {task_id} started"
def stop(task_id):
    if task_id not in tasks:
        return f"Task {task_id} not found "
    task_thread,stop_event=tasks.pop(task_id)
    stop_event.set()
    task_thread.join()
    return f"Task {task_id} stopped"
def process_query_tasks(result, annotation_id, properties, room):
    try:
         
        # Assuming 'result' is your list of records
        if len(result) > 1:  # Check if there are at least 2 records
            record = result[1]  # Get the second record (index 1)
            print("record",record[0])
            node_count_by_label= record[0].get('nodes_count_by_label')
            edge_count_by_type=record[0].get('edges_count_by_type')
            
  
        node_and_edge_count = result[1] if len(result) > 1 else []
        matched_result = result[0]
        print("step -1")
         
        threading.Thread(target=count_nodes_and_edges,args=(node_and_edge_count, annotation_id, room)).start()
        threading.Thread(target=count_by_label_function ,args=(node_count_by_label,edge_count_by_type, annotation_id, room)).start()
        threading.Thread(target=generate_graph,args=(requests, properties, room)).start()
        threading.Thread(target=summary,args=(graph, requests, node_count_by_label, edge_count_by_label, annotation_id, room)).start()
        # graph=asyncio.create_task(generate_graph(matched_result, properties, room)),
        # node_count_by_label, edge_count_by_label=asyncio.create_task(count_by_label_function(node_count_by_label,edge_count_by_type, annotation_id, room)),
        # nodes,edges=asyncio.create_task(count_nodes_and_edges(node_and_edge_count, annotation_id, room))
        
 
        
 
        return graph, node_count_by_label, edge_count_by_label,nodes,edges

     
    except Exception as e:
        socketio.emit('update_event', {"error": f"Error in process_query_tasks: {str(e)}"}, room=room)
        return None, None, None
    
 



def count_nodes_and_edges(node_and_edge_count, annotation_id, room):
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
         
        print("count_nodes_and_edges task completed")
        return node_count, edge_count
    except Exception as e:
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        raise e

def count_by_label_function(node_count_by_label,edge_count_by_type, annotation_id, room):
    
    try:
         
         
        update_annotation = {
            "node_count_by_label": node_count_by_label,
            "edge_count_by_label": edge_count_by_type,
            "updated_at": datetime.datetime.now()
        }
        storage_service.update(annotation_id, update_annotation)

         
        socketio.emit("update_event", {"node_count_by_label": node_count_by_label, "edge_count_by_label": edge_count_by_type}, room=room)
        print("node count by label function ")
         
        return node_count_by_label, edge_count_by_type
    except Exception as e:
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        raise e

def generate_graph(requests, properties, room):
    print("generate grpah function ")
    try:
        request_data = CypherQueryGenerator.graph_function(requests, properties)
        
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        if isinstance(request_data, tuple):
            # Convert tuple to dictionary if necessary
            request_data = {"nodes": request_data[0], "edges": request_data[1]}
        # print('update_event', {"graph": True}, room=room)
        socketio.emit('update_event', {"graph": True}, room=room)
        
        return request_data
     
    except Exception as e:
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        raise e
def summary(graph, requests, node_count_by_label, edge_count_by_label, annotation_id, room):
    try:
        print("summary ")
         
             
        
        summary = llm.generate_summary(graph, requests, node_count_by_label, edge_count_by_label) or 'Graph too big, could not summarize'
        summary = "summary of the graph is here"
        
        updated_annotation = {
            "summary": summary,
            "updated_at": datetime.datetime.now()
        }
        storage_service.update(annotation_id, updated_annotation)
        
        return summary
    
 
    except Exception as e:
        print(f"Exception occurred in summary: {e}")
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        raise
