from app.models.storage import Storage
from pymongoose.mongo_types import MongoException, MongoError
from pymongo.errors import DuplicateKeyError  # Import to catch MongoDB duplicate errors

class StorageService:
    def __init__(self):
        pass

    # Save the query
    def save(self, user_id, query, title, summary):
        try:
            # Check if the user has already saved the same query
            existing_query = Storage.find({"user_id": user_id, "query": query}, one=True)
            if existing_query:
                # Return a clear message when the query already exists
                return {"error": "DuplicateQuery", "message": f"Query already exists for user_id: {user_id}"}

            # If no existing query, create a new entry
            data = Storage(
                user_id=user_id,
                query=query,
                title=title,
                summary=summary
            )
            data.save()
            return {"message": "Query saved successfully"}

        except DuplicateKeyError:
            # Catch the unique index violation (duplicate query for the same user)
            return {"error": "DuplicateQuery", "message": f"Query already exists for user_id: {user_id}"}

        except MongoError as e:
            # Handle MongoDB-specific errors
            return {"error": str(e), "message": "Failed to save query"}

        except Exception as e:
            # Handle any other exceptions
            return {"error": str(e), "message": "An unexpected error occurred"}

    # Retrieve a specific query for the user
    def get(self, user_id):
        data = Storage.find({"user_id": user_id}, one=True)
        return data
    
    # Get all queries for a user with pagination
    def get_all(self, user_id, page_number):
        data = Storage.find({"user_id": user_id}).sort('_id', -1).skip((page_number - 1) * 10).limit(10)
        return data
    
    # Get a query by its ID
    def get_by_id(self, id):
        data = Storage.find_by_id(id)
        return data
