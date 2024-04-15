#-------------------------------------------------------------------------
# AUTHOR:Bryan Martinez Ramirez
# FILENAME: db_connection_mongo.py
# SPECIFICATION: 
# FOR: CS 4250- Assignment #3
# TIME SPENT: 4 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays
from pymongo import MongoClient

def connectDataBase():
    # Connecting to MongoDB (assumes MongoDB is running on localhost with the default port)
    client = MongoClient('mongodb://localhost:27017/')
    return client['your_database_name']  # Return the database instance

def createDocument(col, docId, docText, docTitle, docDate, docCat):
    # Split the document text into terms and count occurrences
    terms = {}
    for term in docText.lower().split():
        if term in terms:
            terms[term] += 1
        else:
            terms[term] = 1

    # Convert term dictionary to a list of dictionaries with additional data
    term_list = [{'term': term, 'term_count': count, 'num_chars': len(term)} for term, count in terms.items()]

    # Create the document
    document = {
        "_id": int(docId),
        "text": docText,
        "title": docTitle,
        "num_chars": len(docText.replace(" ", "").replace(".", "").replace(",", "")),
        "date": docDate,
        "category": {"name": docCat},
        "terms": term_list
    }

    # Insert the document into the collection
    col.insert_one(document)

def deleteDocument(col, docId):
    # Delete the document with the given ID
    col.delete_one({"_id": int(docId)})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    # First delete the document
    deleteDocument(col, int(docId))

    # Then recreate it with the updated details
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):
    # Create an inverted index for the terms across all documents
    cursor = col.aggregate([
        {"$unwind": "$terms"},
        {"$group": {
            "_id": "$terms.term",
            "docs": {
                "$push": {
                    "title": "$title",
                    "count": "$terms.term_count"
                }
            }
        }}
    ])
    inverted_index = {}
    for document in cursor:
        term = document["_id"]
        doc_list = ', '.join([f"{d['title']}:{d['count']}" for d in document['docs']])
        inverted_index[term] = doc_list
    return inverted_index

if __name__ == '__main__':
    db = connectDataBase()
    documents = db.documents

    print("")
    print("######### Menu ##############")
    print("#a - Create a document")
    print("#b - Update a document")
    print("#c - Delete a document.")
    print("#d - Output the inverted index.")
    print("#e - Quit")

    option = ""
    while option != "e":
        print("")
        option = input("Enter a menu choice: ")

        if option == "a":
            docId = input("Enter the ID of the document: ")
            docText = input("Enter the text of the document: ")
            docTitle = input("Enter the title of the document: ")
            docDate = input("Enter the date of the document: ")
            docCat = input("Enter the category of the document: ")
            createDocument(documents, docId, docText, docTitle, docDate, docCat)

        elif option == "b":
            docId = input("Enter the ID of the document: ")
            docText = input("Enter the text of the document: ")
            docTitle = input("Enter the title of the document: ")
            docDate = input("Enter the date of the document: ")
            docCat = input("Enter the category of the document: ")
            updateDocument(documents, docId, docText, docTitle, docDate, docCat)

        elif option == "c":
            docId = input("Enter the document id to be deleted: ")
            deleteDocument(documents, docId)

        elif option == "d":
            index = getIndex(documents)
            print(index)

        elif option == "e":
            print("Leaving the application ... ")
        else:
            print("Invalid Choice.")
