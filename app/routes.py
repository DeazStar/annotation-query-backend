from flask import copy_current_request_context, request, jsonify, Response, send_from_directory
import neo4j 
import traceback
import asyncio
import re
import logging
import json
import yaml
import os
import threading
from app import app, databases, schema_manager, db_instance 
from app.lib import validate_request
 
from app.lib import limit_graph
from app.lib.auth import token_required
from app.lib.email import init_mail, send_email
from dotenv import load_dotenv
from distutils.util import strtobool
import datetime
from app.lib import convert_to_csv
from app.lib import generate_file_path
from app.lib import adjust_file_path
from flask_socketio import join_room, leave_room,emit,send
import redis
from app.services.cypher_generator import CypherQueryGenerator
from run import socketio
from threading import Lock


redis_client=redis.Redis(host='localhost',port=6379,db=0,decode_responses=True)
# Load environmental variables
load_dotenv()
# Set the allowed origin for WebSocket connections
current_user_id=None
def handle_message(auth):
    emit('my responce',{'data':"Connected"})
 
# set mongo loggin
logging.getLogger('pymongo').setLevel(logging.CRITICAL)

# Flask-Mail configuration
app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER') 
app.config['MAIL_PORT'] = os.getenv('MAIL_PORT')
app.config['MAIL_USE_TLS'] = bool(strtobool(os.getenv('MAIL_USE_TLS')))
app.config['MAIL_USE_SSL'] = bool(strtobool(os.getenv('MAIL_USE_SSL')))
app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME')
app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')
app.config['MAIL_DEFAULT_SENDER'] = os.getenv('MAIL_DEFAULT_SENDER')

llm = app.config['llm_handler']
storage_service = app.config['storage_service']
annotation={}
# Initialize Flask-Mail
init_mail(app)

 

# Setup basic logging
logging.basicConfig(level=logging.DEBUG)

@app.route('/kg-info', methods=['GET'])
@token_required
def get_graph_info(current_user_id):
    graph_info = json.dumps(schema_manager.graph_info, indent=4)
    return Response(graph_info, mimetype='application/json')

@app.route('/nodes', methods=['GET'])
@token_required
def get_nodes_endpoint(current_user_id):
    nodes = json.dumps(schema_manager.get_nodes(), indent=4)
    return Response(nodes, mimetype='application/json')

@app.route('/edges', methods=['GET'])
@token_required
def get_edges_endpoint(current_user_id):
    
    edges = json.dumps(schema_manager.get_edges(), indent=4)
    return Response(edges, mimetype='application/json')

@app.route('/relations/<node_label>', methods=['GET'])
@token_required
def get_relations_for_node_endpoint(current_user_id, node_label):
    relations = json.dumps(schema_manager.get_relations_for_node(node_label), indent=4)
    return Response(relations, mimetype='application/json')
@socketio.on('connect')
def handle_connect():
    print('Client connected')

@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

@socketio.on('join')
def on_join(data):
    try:
        user_id = current_user_id
        room = data['room']
        join_room(room)
        socketio.emit('status', {"status": "connected", "message": f"{user_id} has joined the room {room}"}, room=room)
    except Exception as e:
        socketio.emit('status', {"status": "error", "message": f"Failed to join room: {str(e)}"}, room=room)

@socketio.on('leave')
def on_leave(data):
    user_id = current_user_id
    room = data['room']
    leave_room(room)
    socketio.emit('status', {"status": "disconnected", "message": f"{user_id} has left the room {room}"}, room=room)

running_processes_lock = Lock()  
task_tracker = {
    "graph": False,
    "node_count_by_label": False,
    "edge_count_by_label": False,
    "node_count": False,
    "edge_count": False,
    "summary": False
}

def check_all_tasks_completed():
    """Check if all tasks are completed."""
    print(task_tracker.values())
    return all(task_tracker.values())

def reset_task_tracker():
    """Reset the task tracker."""
    for key in task_tracker:
        task_tracker[key] = False
running_processes={}
@app.route('/query', methods=['POST'])
@token_required
def process_query(current_user_id):
    global room
    current_user_id = current_user_id

    print("1. Starting process_query function")  # Debug print

    data = request.get_json()
    print(f"2. Received data: {data}")  # Debug print

    annotation_id = data['requests'].get('annotation_id', None)
    room = annotation_id
    print(f"3. Annotation ID: {annotation_id}, Room: {room}")  # Debug print

    if annotation_id in running_processes and running_processes[annotation_id].get('cancelled', False):
        print("4. Task has been cancelled")  # Debug print
        return jsonify({"error": "Task has been cancelled"}), 400
     
    # if annotation_id and redis_client.exists(annotation_id):
    #     cached_data = redis_client.get(annotation_id)
    #     cached_data = json.loads(cached_data)
    #     print("5. Returning cached data")  # Debug print
    #     return jsonify(cached_data)

    async def _process_query():
        try:
            print("6. Starting _process_query coroutine")  # Debug print
            data = request.get_json()
            print(f"7. Received data in _process_query: {data}")  # Debug print

            if not data or 'requests' not in data:
                print("8. Missing requests data")  # Debug print
                return jsonify({"error": "Missing requests data"}), 400
            
            annotation_id = data['requests'].get('annotation_id', None)
            print(f"9. Annotation ID in _process_query: {annotation_id}")  # Debug print
            
            limit = request.args.get('limit')
            properties = request.args.get('properties')
            source = request.args.get('source')
            if properties:
                properties = bool(strtobool(properties))
            else:
                properties = False

            if limit:
                try:
                    limit = int(limit)
                except ValueError:
                    print("10. Invalid limit value")  # Debug print
                    return jsonify({"error": "Invalid limit value. It should be an integer."}), 400
            else:
                limit = None

            requests = data['requests']
            print(f"11. Requests: {requests}")  # Debug print

            node_map = validate_request(requests, schema_manager.schema)
            if node_map is None:
                print("12. Invalid node_map returned by validate_request")  # Debug print
                return jsonify({"error": "Invalid node_map returned by validate_request"}), 400
         
            annotation = {
                    "current_user_id": str(current_user_id),
                    "requests": requests,
                    "query": '',
                    "question": "",
                    "title": "",
                    "answer": "",
                    "summary": "",
                    "node_count": 0,
                    "edge_count": 0,
                    "node_count_by_label": 0,
                    "edge_count_by_label": 0,
                    "node_types": ""
                }
        
            # annotation_id = str(storage_service.save(annotation))
            print(f"13. Annotation saved with ID: {annotation_id}")  # Debug print

            with running_processes_lock:
                task = asyncio.current_task()
                running_processes[annotation_id] = {"task": task, "cancelled": False}
           
            socketio.emit("update_event", {"status": "pending", "annotation_id": annotation_id})
            print("14. Emitted update_event with pending status")  # Debug print

            requests = db_instance.parse_id(requests)
            query_code = db_instance.query_Generator(requests, node_map)
            print(f"15. Generated query code: ***")  # Debug print

            result = db_instance.run_query(query_code,source)
            print(f"16. Query result: ****")  # Debug print

            room = annotation_id


            print("room_______________________",room)

            # Process tasks
            graph, node_count_by_label, edge_count_by_label = await process_query_tasks(result, annotation_id, properties, room)
            print(f" Node count by label: {node_count_by_label}, Edge count by label: {edge_count_by_label}")  # Debug print

            # Save to Redis
            if annotation_id != 'None':
                redis_client.setex(annotation_id, 7200, json.dumps(graph))
                print("18. Saved data to Redis")  # Debug print

            # Generate summary
            summary_val = await summary(graph, requests, node_count_by_label, edge_count_by_label, annotation_id, room)
            print(f"19. Generated summary: {summary_val}")  # Debug print

            # Emit summary
            socketio.emit("update_event", {"status": "pending", "summary": summary_val}, room=room)
            print("20. Emitted update_event with summary")  # Debug print

            task_tracker["summary"] = True

            # Check if all tasks are completed
            if check_all_tasks_completed():
                socketio.emit('update_event', {"status": "completed", "message": "All tasks have been processed"}, room=room)
                socketio.emit('close_connection', {"message": "Connection will now close"}, room=room)
                print("21. All tasks completed, emitted update_event and close_connection")  # Debug print

            return jsonify({"requests": requests, "annotation_id": str(annotation_id)})
        except asyncio.CancelledError:
            print("22. Task has been cancelled")  # Debug print
            socketio.emit('update_event', {"status": "cancelled", "message": "Task has been cancelled"}, room=room)
            return jsonify({"error": "Task has been cancelled"}), 400
        except Exception as e:
            room = annotation_id
            print("room_____",room)
            print(f"23. Exception occurred: {e}")  # Debug print
            traceback.print_exc()
            socketio.emit('update_event', {"status": "error", "message": "error happened in the graph"}, room=room)
            return jsonify({"error": str(e)}), 500
        finally:
            print("24. Cleaning up running_processes and resetting task_tracker")  # Debug print
            with running_processes_lock:
                if annotation_id in running_processes:
                    del running_processes[annotation_id]
            reset_task_tracker()
     
    return asyncio.run(_process_query())
     
async def process_query_tasks(result, annotation_id, properties, room):
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
        tasks = [
            asyncio.create_task(generate_graph(matched_result, properties, room)),
            asyncio.create_task(count_by_label_function(node_count_by_label,edge_count_by_type, annotation_id, room)),
            asyncio.create_task(count_nodes_and_edges(node_and_edge_count, annotation_id, room))
        ]

        print("step -2")
        # while not check_all_tasks_completed():
        #     with running_processes_lock:
        #         if running_processes.get(annotation_id, {}).get('cancelled', False):
        #             raise asyncio.CancelledError("Task has been cancelled")
        #     await asyncio.sleep(1)  # Sleep to avoid busy-waiting
        # print("step -3")
        # print("Step before asyncio.gather")
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    print(f"Task failed with exception: {result}")
        except Exception as e:
            print(f"asyncio.gather failed with exception: {e}")

        graph = results[0]
        node_count_by_label, edge_count_by_label = results[1]
        return graph, node_count_by_label, edge_count_by_label

    except asyncio.CancelledError:
         
        print(f"Task with annotation_id {annotation_id} has been cancelled")
        socketio.emit('update_event', {"status": "cancelled", "message": "Task has been cancelled"}, room=room)
        return None, None, None
    except Exception as e:
        socketio.emit('update_event', {"error": f"Error in process_query_tasks: {str(e)}"}, room=room)
        return None, None, None
    
"""Each query task is tracked by its annotation_id in the running_processes dictionary.
/cancel endpoint sets the cancelled flag for the task and calls task.cancel().
he task checks for cancellation, raises asyncio.CancelledError
""" 
@app.route('/cancel', methods=['POST'])
@token_required
def cancel_task(current_user_id):
    data = request.get_json()
    print("running in cancel",running_processes)
     
    
    annotation_id = data['requests']['annotation_id'] 
    
    if not annotation_id:
        return jsonify({"error": "Missing annotation_id"}), 400

    with running_processes_lock:
        if annotation_id in running_processes:
            task_info = running_processes[annotation_id]
            if not task_info.get('cancelled', False):
                task_info["cancelled"] = True
                task = task_info.get("task")
               
                if task and not task.done():
                    task.cancel()  # Cancel the task
                    return jsonify({"message": f"Task with annotation_id {annotation_id} has been cancelled"}), 200
            else:
                return jsonify({"error": f"Task with annotation_id {annotation_id} is already cancelled"}), 400
        else:
            return jsonify({"error": f"No running task found with annotation_id {annotation_id}"}), 404

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

        if isinstance(request_data, tuple):
            # Convert tuple to dictionary if necessary
            request_data = {"nodes": request_data[0], "edges": request_data[1]}
        # print('update_event', {"graph": True}, room=room)
        socketio.emit('update_event', {"graph": True}, room=room)
        task_tracker["graph"] = True
        return request_data
    except Exception as e:
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        raise e

async def summary(graph, requests, node_count_by_label, edge_count_by_label, annotation_id, room):
    try:
        print("summary ")
        # summary = llm.generate_summary(graph, requests, node_count_by_label, edge_count_by_label) or 'Graph too big, could not summarize'
        summary = "summary of the graph is here"
        updated_annotation = {
            "summary": summary,
            "updated_at": datetime.datetime.now()
        }
        storage_service.update(annotation_id, updated_annotation)
        return summary
    except Exception as e:
        socketio.emit('update_event', {"status": "error", "message": str(e)}, room=room)
        raise e
  
@app.route('/history', methods=['GET'])
@token_required
def process_user_history(current_user_id):
    page_number = request.args.get('page_number')
    if page_number is not None:
        page_number = int(page_number)
    else:
        page_number = 1
    return_value = []
    cursor = storage_service.get_all(str(current_user_id), page_number)

    if cursor is None:
        return jsonify('No value Found'), 200

    for document in cursor:
        return_value.append({
            'annotation_id': str(document['_id']),
            'title': document['title'],
            'node_count': document['node_count'],
            'edge_count': document['edge_count'],
            'node_types': document['node_types'],
            "created_at": document['created_at'].isoformat(),
            "updated_at": document["updated_at"].isoformat()
        })
    return Response(json.dumps(return_value, indent=4), mimetype='application/json')

@app.route('/annotation/<id>', methods=['GET'])
@token_required
def process_by_id(current_user_id, id):
    cursor = storage_service.get_by_id(id)

    if cursor is None:
        return jsonify('No value Found'), 200
    query = cursor.query
    title = cursor.title
    summary = cursor.summary
    annotation_id = cursor.id
    question = cursor.question
    answer = cursor.answer
    node_count = cursor.node_count
    edge_count = cursor.edge_count

    limit = request.args.get('limit')
    properties = request.args.get('properties')
    
    if properties:
        properties = bool(strtobool(properties))
    else:
        properties = False
 
    if limit:
        try:
            limit = int(limit)
        except ValueError:
            return jsonify({"error": "Invalid limit value. It should be an integer."}), 400
    else:
        limit = None


    try: 
       
        query=query.replace("{PLACEHOLDER}",str(limit)) 
       
        # Run the query and parse the results
        result = db_instance.run_query(query)
      
        response_data = db_instance.parse_and_serialize(result, schema_manager.schema, properties)
        
        response_data["annotation_id"] = str(annotation_id)
        response_data["title"] = title
        response_data["summary"] = summary
        response_data["node_count"] = node_count
        response_data["edge_count"] = edge_count

        if question:
            response_data["question"] = question

        if answer:
            response_data["answer"] = answer

        # if limit:
            # response_data = limit_graph(response_data, limit)

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype='application/json')
    except Exception as e:
        logging.error(f"Error processing query: {e}")
        return jsonify({"error": str(e)}), 500
    




@app.route('/annotation/<id>/full', methods=['GET'])
@token_required
def process_full_annotation(current_user_id, id):
    try:
        link = process_full_data(current_user_id=current_user_id, annotation_id=id)
        if link is None:
            return jsonify('No value Found'), 200

        response_data = {
            'link': link
        }

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype='application/json')
    except Exception as e:
        logging.error(f"Error processing query: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/public/<file_name>')
def serve_file(file_name):
    public_folder = os.path.join(os.getcwd(), 'public')
    return send_from_directory(public_folder, file_name)

def process_full_data(current_user_id, annotation_id):
    cursor = storage_service.get_by_id(annotation_id)

    if cursor is None:
        return None
    query, title = cursor.query, cursor.title
    #remove the limit 
    import re
    if "LIMIT" in query:
        query = re.sub(r'\s+LIMIT\s+\d+', '', query)
     

     
    
    try:
        file_path = generate_file_path(file_name=title, user_id=current_user_id, extension='xls')
        exists = os.path.exists(file_path)

        if exists:
            file_path = adjust_file_path(file_path)
            link = f'{request.host_url}{file_path}'

            return link
        
        # Run the query and parse the results
        # query code inputs 2 value so source=None
        result = db_instance.run_query(query,source=None)
        print("step2 ")
        parsed_result = db_instance.convert_to_dict(result, schema_manager.schema)

        file_path = convert_to_csv(parsed_result, user_id= current_user_id, file_name=title)
        file_path = adjust_file_path(file_path)


        link = f'{request.host_url}{file_path}'
        return link

    except Exception as e:
            raise e

@app.route('/annotation/<id>', methods=['DELETE'])
@token_required
def delete_by_id(current_user_id, id):
    try:
        existing_record = storage_service.get_by_id(id)

        if existing_record is None:
            return jsonify('No value Found'), 404
        
        deleted_record = storage_service.delete(id)

        if deleted_record is None:
            return jsonify('Failed to delete the annotation'), 500
        
        response_data = {
            'message': 'Annotation deleted successfully'
        }

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype='application/json')
    except Exception as e:
        logging.error(f"Error deleting annotation: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/annotation/<id>/title', methods=['PUT'])
@token_required
def update_title(current_user_id, id):
    data = request.get_json()

    if 'title' not in data:
        return jsonify({"error": "Title is required"}), 400

    title = data['title']

    try:
        existing_record = storage_service.get_by_id(id)

        if existing_record is None:
            return jsonify('No value Found'), 404

        updated_data = storage_service.update(id,{'title': title})
        
        response_data = {
            'message': 'title updated successfully',
            'title': title,
        }

        formatted_response = json.dumps(response_data, indent=4)
        return Response(formatted_response, mimetype='application/json')
    except Exception as e:
        logging.error(f"Error updating title: {e}")
        return jsonify({"error": str(e)}), 500