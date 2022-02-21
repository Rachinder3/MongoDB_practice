from logger.logger import Logger
import pymongo

"""module implementing the MongoDB class and its functions"""

class mongodb:
    __log_obj = Logger(filename="Logs\\mongoDB.log")

    def __init__(self, username, password):
        try:
            """constructor of MongoDB class. Creates the connection with MongoDB cluster and establishes connection 
            with the client. """
            client = pymongo.MongoClient(
                "mongodb+srv://{}:{}@cluster0.evjea.mongodb.net/myFirstDatabase?retryWrites=true&w=majority".format(
                    username, password))
            self.db = client.test
            mongodb.__log_obj.add_log("mongo db object initialized, connection established with the cluster")
        except Exception as e:
            mongodb.__log_obj.add_log("problems in connecting with cluster.", mode="error")
            mongodb.__log_obj.add_log(str(e))

    # Create functions
    def create_collection(self, collection_name):
        try:
            """function to create a new collection within the cluster."""
            collection = self.db[collection_name]
            mongodb.__log_obj.add_log("collection created")
            return collection
        except Exception as e:
            mongodb.__log_obj.add_log("problems creating the cluster.")
            mongodb.__log_obj.add_log(str(e))

    def insert_single_documents(self, collection, document):
        try:
            """function to insert a single document in the collection."""
            collection.insert_one(document)
            mongodb.__log_obj.add_log("document inserted")
        except Exception as e:
            mongodb.__log_obj.add_log("Problem in inserting the document into the collection.")
            mongodb.__log_obj.add_log(str(e))

    def insert_multiple_documents(self, collection, documents):
        try:
            """function to insert multiple documents in the cluster."""
            collection.insert_many(documents)
            mongodb.__log_obj.add_log("documents inserted.")
        except Exception as e:
            mongodb.__log_obj.add_log("Problems in inserting documents inside the collection.")
            mongodb.__log_obj.add_log(str(e))

    # Read functions
    def fetch_docs(self, collection, condition=None, limit=-1):
        try:
            """implements the Read functionality. """
            if condition != None:
                if limit == -1:
                    return collection.find(condition)
                else:
                    return collection.find(condition).limit(limit)
            else:
                if limit == -1:
                    return collection.find()
                else:
                    return collection.find().limit(limit)

        except Exception as e:
            mongodb.__log_obj.add_log("Problems in fetching data")
            mongodb.__log_obj.add_log(str(e))

    # update functions
    def update_docs(self, collection, previous, new):
        try:
            """function that implements the update functionality."""
            collection.update_many(previous, new)
            mongodb.__log_obj.add_log("documents updated")

        except Exception as e:
            mongodb.__log_obj.add_log("Problems in updating documents")
            mongodb.__log_obj.add_log(str(e))

    # delete functions
    def delete_single_docs(self, collection, condition):
        try:
            """function that implements the delete single document functionality."""
            collection.delete_one(condition)
            mongodb.__log_obj.add_log("Document deleted")
        except Exception as e:
            mongodb.__log_obj.add_log("Problems in deleting documents")
            mongodb.__log_obj.add_log(str(e))

    def delete_multiple_docs(self, collection, condition):
        try:
            """function that implements deleting multiple documents functionality"""
            collection.delete_many(condition)
            mongodb.__log_obj.add_log("Documents deleted")
        except Exception as e:
            mongodb.__log_obj.add_log("Problems in deleting documents")
            mongodb.__log_obj.add_log(str(e))

    def drop_collection(self, collection):
        try:
            """function that helps in dropping the complete collection."""
            collection.drop()
            mongodb.__log_obj.add_log("Collection dropped")
        except Exception as e:
            mongodb.__log_obj.add_log("Problems in dropping collection.")
            mongodb.__log_obj.add_log(str(e))
