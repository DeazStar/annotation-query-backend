from flask import request
from app.routes import process_query
from app.socket import socketio

@socketio.on('query')
def handle_query(current_user_id, data, limit, properties):
    process_query(current_user_id, data, limit, properties, request.sid)
