import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mydatabase"]
artists = db['artists']

cursor = artists.find({})
print('\nList of artists')
for document in cursor:
    print(document)

#artists.drop()