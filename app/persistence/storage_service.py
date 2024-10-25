from app.models.storage import Storage
from pymongoose.mongo_types import   MongoException, MongoError
class StorageService:
    def __init__(self):
        pass
    #cheack
    def save(self, user_id, query, title, summary):
        try:
            # Check if the user has already saved the same query
            existing_query = Storage.find({"user_id": user_id, "query": query}, one=True)
            if existing_query:
                raise MongoException(f"Query already exists for user_id: {user_id}")
            
            # If no existing query, create a new entry
            data = Storage(
                user_id=user_id,
                query=query,
                title=title,
                summary=summary
            )
            data.save()
            return {"message": "Query saved successfully"}
        
        except MongoError as e:
            # Handle MongoDB specific errors
            return {"error": str(e), "message": "Failed to save query"}
        except Exception as e:
            # Handle any other exceptions
            return {"error": str(e), "message": "An unexpected error occurred"}
    
    def get(self, user_id):
        try:
            data = Storage.find({"user_id": user_id}, one=True)
            if not data:
                raise MongoException(f"No data found for user_id: {user_id}")
            return data
        except MongoError as e:
            return {"error": str(e), "message": "Failed to retrieve data"}
        except Exception as e:
            return {"error": str(e), "message": "An unexpected error occurred"}
    
    def get_all(self, user_id, page_number):
        try:
            data = Storage.find({"user_id": user_id}).sort('_id', -1).skip((page_number - 1) * 10).limit(10)
            return data
        except MongoError as e:
            return {"error": str(e), "message": "Failed to retrieve data"}
        except Exception as e:
            return {"error": str(e), "message": "An unexpected error occurred"}
    
    def get_by_id(self, id):
        try:
            data = Storage.find_by_id(id)
            if not data:
                raise MongoException(f"No data found with id: {id}")
            return data
        except MongoError as e:
            return {"error": str(e), "message": "Failed to retrieve data"}
        except Exception as e:
            return {"error": str(e), "message": "An unexpected error occurred"}
