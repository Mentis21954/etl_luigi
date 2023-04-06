import pymongo

client = pymongo.MongoClient("mongodb+srv://user:AotD8lF0WspDIA4i@cluster0.qtikgbg.mongodb.net/?retryWrites=true&w=majority")
db = client["mydatabase"]
artists = db['artists']

cursor = artists.find({})
print('\nList of artists')
for document in cursor:
    print(document)