from app.models.storage import Storage
from pymongoose.mongo_types import MongoException, MongoError
from pymongo.errors import DuplicateKeyError  # Import to catch MongoDB duplicate errors

class StorageService:
    def __init__(self):
        pass

    # Save the query
    def save(self, user_id, query, title, summary):
        try:
           
            existing_query = Storage.find({"user_id": user_id, "query": query}, one=True)
            if existing_query:
                 
                return {"error": "DuplicateQuery", "message": f"Query already exists for user_id: {user_id}"}

        
            data = Storage(
                user_id=user_id,
                query=query,
                title=title,
                summary=summary
            )
            data.save()
            return {"message": "Query saved successfully"}

        except DuplicateKeyError:
            
            return {"error": "DuplicateQuery", "message": f"Query already exists for user_id: {user_id}"}

        except MongoError as e:
        
            return {"error": str(e), "message": "Failed to save query"}

        except Exception as e:
        
            return {"error": str(e), "message": "An unexpected error occurred"}


    def get(self, user_id):
        data = Storage.find({"user_id": user_id}, one=True)
        return data
    
    def get_all(self, user_id, page_number):
        data = Storage.find({"user_id": user_id}).sort('_id', -1).skip((page_number - 1) * 10).limit(10)
        return data
    
    def get_by_id(self, id):
        data = Storage.find_by_id(id)
        return data
