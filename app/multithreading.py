import threading
import socketio
import datetime
from flask import Flask, jsonify, request
from threading import Thread, Event
import time
from app.services.cypher_generator import CypherQueryGenerator
from app import app, databases, schema_manager, db_instance 

llm = app.config['llm_handler']
storage_service = app.config['storage_service']

# Flask app
app = Flask(__name__)

# Global variables
exit_event = threading.Event()
tasks = {}
threads = []
stop_event = Event()

def run_query(query_code, stop_event, results):
    """Executes a query and stores results."""
    try:
        with db_instance.session() as session:
            result = session.execute_read(lambda tx: tx.run(query_code))
            for record in result:
                if stop_event.is_set():
                    break
                results.append(record)
    except Exception as e:
        results.append({"error": "run_query", "error_value": str(e)})

def generate_graph(requests, properties, room):
    try:
        request_data = CypherQueryGenerator.graph_function(requests, properties)
        socketio.emit('update_event', {"graph": True}, room=room)
        return request_data
    except Exception as e:
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        raise e

def count_by_label_function(node_count_by_label, edge_count_by_type, annotation_id, room):
    try:
        node_count, edge_count = CypherQueryGenerator.count_by_label_function(node_count_by_label)
        update_annotation = {
            "node_count_by_label": node_count,
            "edge_count_by_label": edge_count,
            "updated_at": datetime.datetime.now()
        }
        storage_service.update(annotation_id, update_annotation)
        socketio.emit("update_event", update_annotation, room=room)
        return node_count, edge_count
    except Exception as e:
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        raise e

def count_nodes_and_edges(results, annotation_id, room):
    try:
        if not results:
            raise ValueError("No results available for counting nodes and edges.")
        node_count, edge_count = CypherQueryGenerator.count_node_and_edges_function(results)
        update_annotation = {
            "node_count": node_count,
            "edge_count": edge_count,
            "updated_at": datetime.datetime.now()
        }
        storage_service.update(annotation_id, update_annotation)
        socketio.emit("update_event", update_annotation, room=room)
        if stop_event.is_set():
            print("Counting canceled.")
            return None
        return node_count, edge_count
    except Exception as e:
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        raise e

def generate_summary(graph, requests, node_count_by_label, edge_count_by_label, annotation_id, room):
    if stop_event.is_set():
        print("Summary generation canceled.")
        return None
    try:
        summary = llm.generate_summary(graph, requests, node_count_by_label, edge_count_by_label)
        if not summary:
            summary = "Graph too big, could not summarize"
        update_annotation = {
            "summary": summary,
            "updated_at": datetime.datetime.now()
        }
        storage_service.update(annotation_id, update_annotation)
        socketio.emit("update_event", {"status": "pending", "summary": summary}, room=room)
        return summary
    except Exception as e:
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        raise e

def execute_graph_tasks():
    """Executes tasks in proper sequence with dependencies."""
    results = []
    output = {}
    
    query_thread = Thread(target=run_query, args=(query_code, stop_event, results))
    threads.append(query_thread)
    query_thread.start()
    query_thread.join()
    
    if not results:
        print("No query results available.")
        return
    
    graph_thread = Thread(target=generate_graph, args=(results, output, 'graph_room'))
    count_thread = Thread(target=count_by_label_function, args=(results, output, 'annotation_id', 'count_room'))
    nodes_edges_thread = Thread(target=count_nodes_and_edges, args=(results, 'annotation_id', 'count_room'))
    
    threads.extend([graph_thread, count_thread, nodes_edges_thread])
    graph_thread.start()
    count_thread.start()
    nodes_edges_thread.start()
    
    graph_thread.join()
    count_thread.join()
    nodes_edges_thread.join()
    
    if not stop_event.is_set():
        summary_thread = Thread(target=generate_summary, args=(output.get("graph"), output.get("requests"), output.get("node_count_by_label"), output.get("edge_count_by_label"), 'annotation_id', 'summary_room'))
        threads.append(summary_thread)
        summary_thread.start()
        summary_thread.join()

@app.route('/start', methods=['POST'])
def start_tasks():
    """Starts the tasks in a separate thread."""
    global stop_event, threads
    stop_event.clear()
    main_thread = Thread(target=execute_graph_tasks)
    threads.append(main_thread)
    main_thread.start()
    return jsonify({"message": "Tasks started"})

@app.route('/cancel', methods=['POST'])
def cancel_tasks():
    """Stops all running tasks."""
    global stop_event
    stop_event.set()
    for thread in threads:
        if thread.is_alive():
            print(f"Stopping thread {thread.name}")
    threads.clear()
    return jsonify({"message": "Tasks canceled"})

if __name__ == '__main__':
    app.run(debug=True)
