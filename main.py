from file_manipulation.File_Manipulation import File_Manipulation
from MongoDB.MongoDB import mongodb

if __name__ == "__main__":
    url = "https://archive.ics.uci.edu/ml/machine-learning-databases/00448/carbon_nanotubes.csv"
    filepath = "Downloads/carbon_nanotubes.csv"
    username = "mongodb"
    password = "mongodb"

    file_manipulation_obj = File_Manipulation(url, filepath)

    # Connecting with mongodb cluster
    mongodb_obj = mongodb(username, password)

    # Downloading the file
    file_manipulation_obj.Download_file()

    # Creating data dictionary list
    data_dictionary_list = file_manipulation_obj.create_data_dictionary_list()

    # Creating collection
    collection = mongodb_obj.create_collection("carbon nanotubes")

    # inserting data inside collection
    mongodb_obj.insert_multiple_documents(collection, data_dictionary_list)

    # inserting single document

    query = {'name': "Rachinder", "class": "FSDS"}
    mongodb_obj.insert_single_documents(collection=collection, document=query)

    # update

    previous = {'name': "Rachinder"}
    new = {"$set": {"name": "Singh"}}

    mongodb_obj.update_docs(collection=collection, previous=previous, new=new)

    # read
    data = mongodb_obj.fetch_docs(collection=collection, condition={'name': "Singh"})
    for i in data:
        print(i)

    # delete
    query = {"name": "Singh"}
    mongodb_obj.delete_single_docs(collection=collection, condition=query)

    # drop the collection
    mongodb_obj.drop_collection(collection=collection)
