from app import app
from app.events import socketio
from dotenv import load_dotenv
import os

load_dotenv()

APP_PORT = os.getenv('APP_PORT')

if __name__ == '__main__':
    # app.run(debug=True, host='0.0.0.0', port=APP_PORT)
    socketio.init_app(app, cors_allowed_origins="*")
    socketio.run(app, debug=True)
    
    
