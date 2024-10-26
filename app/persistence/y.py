from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/save_query', methods=['POST'])
def save_query():
    from storage_service import StorageService  # Move import inside the function
    storage_service = StorageService()

    data = request.json
    user_id = data.get('user_id')
    query = data.get('query')
    title = data.get('title')
    summary = data.get('summary')

    if not user_id or not query or not title or not summary:
        return jsonify({"error": "Missing required fields"}), 400

    result = storage_service.save(user_id, query, title, summary)
    return jsonify(result), 200

@app.route('/get_query/<user_id>', methods=['GET'])
def get_query(user_id):
    from storage_service import StorageService  # Import here too
    storage_service = StorageService()  # Create an instance
    result = storage_service.get(user_id)
    return jsonify(result), 200

@app.route('/get_all_queries/<user_id>/<int:page_number>', methods=['GET'])
def get_all_queries(user_id, page_number):
    from storage_service import StorageService
    storage_service = StorageService()
    result = storage_service.get_all(user_id, page_number)
    return jsonify(result), 200

@app.route('/get_query_by_id/<query_id>', methods=['GET'])
def get_query_by_id(query_id):
    from storage_service import StorageService
    storage_service = StorageService()
    result = storage_service.get_by_id(query_id)
    return jsonify(result), 200

if __name__ == '__main__':
    app.run(debug=True)
