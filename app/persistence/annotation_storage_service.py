from app.models.annotation import Annotation


class AnnotationStorageService:
    def __init__(self):
        pass

    @staticmethod
    def save(annotation):
        data = Annotation(
            request=annotation["request"],
            query=annotation["query"],
            title=annotation["title"],
            summary=annotation.get("summary", None),
            question=annotation.get("question", None),
            answer=annotation.get("answer", None),
            node_count=annotation.get("node_count", None),
            edge_count=annotation.get("edge_count", None),
            node_types=annotation["node_types"],
            node_count_by_label=annotation.get("node_count_by_label", None),
            edge_count_by_label=annotation.get("edge_count_by_label", None),
            status=annotation.get("status", "PENDING"),
        )

        id = data.save()
        return id

    @staticmethod
    def get_all(page_number=1):
        data = Annotation.find().sort("_id", -1).skip((page_number - 1) * 10).limit(10)
        return data

    @staticmethod
    def get_by_id(id):
        data = Annotation.find_by_id(id)
        return data

    @staticmethod
    def get_query(annotation_id, query):
        data = Annotation.find_one({"_id": annotation_id, "query": query})
        return data

    @staticmethod
    def get_annotation(annotation_id):
        data = Annotation.find_one({"_id": annotation_id})
        return data

    @staticmethod
    def update(id, data):
        data = Annotation.update({"_id": id}, {"$set": data}, many=False)
        return data

    @staticmethod
    def delete(id):
        data = Annotation.delete({"_id": id})
        return data

    @staticmethod
    def delete_many_by_id(ids):
        delete_count = 0

        for id in ids:
            AnnotationStorageService.delete(id)
            delete_count += 1

        return delete_count
