from app.lib.canonicalizer import Canonicalizer
from app.persistence.annotation_storage_service import AnnotationStorageService
from app.constants import TaskStatus

class QueryDeduplicator:
    @staticmethod
    def get_existing_result(request_data, species, data_source):
        """
        Checks if a result for the given query already exists in the system.
        
        Args:
            request_data (dict): The 'requests' object from the frontend.
            species (str): The species context.
            data_source (str): The data source context.
            
        Returns:
            Annotation: The existing annotation document if found, else None.
            str: The query hash.
        """
        query_hash = Canonicalizer.canonicalize(request_data, species, data_source)
        
        # Check MongoDB for any previous annotation with this hash
        existing_annotation = AnnotationStorageService.find_by_hash(query_hash)
        
        # We ONLY share results that are already COMPLETE.
        if existing_annotation and existing_annotation.status == TaskStatus.COMPLETE.value:
            return existing_annotation, query_hash
                
        return None, query_hash
