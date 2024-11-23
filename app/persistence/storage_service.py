from app.models.storage import Storage
from pymongoose.mongo_types import MongoException
import logging
class StorageService():
    def __init__(self):
        pass
    """the other thing is create a new endpoint 
    to allow user to edit the title and the query (or the request)"""
    def save(self, user_id, data, title, summary):
        data = Storage(
                user_id=user_id,
                query=data,
                title=title,
                summary=summary
                )
 
        try:
            data.save()  # Saving to MongoDB
            
        except MongoException as e:
            logging.error(f"Error saving data: {e}")
            raise
    def get(self, user_id):
        data = Storage.find({"user_id": user_id}, one=True)
        return data
    
    def get_all(self, user_id, page_number):
        data = Storage.find({"user_id": user_id}).sort('_id', -1).skip((page_number - 1) * 10).limit(10)
        return data
    
    def get_by_id(self, id):
        data = Storage.find_by_id(id)
        return data

    def get_user_query(self, user_id, query):
        data = Storage.find_one({"user_id": user_id, "query": query})
        
        return data
    
    def update(self, id, data):
        data = Storage.update({"_id": id}, {"$set": data}, many=False)
    def delete_annotation(self,annotation_id):
         
        try:
            result = Storage.delete({"_id": annotation_id})
            return True    
        except MongoException as e:
            logging.error(f"Database error while deleting annotation: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error while deleting annotation: {e}")
            return False
