import dbConfig
import pymongo
import hashlib
import random
connection=pymongo.MongoClient()
db=connection.api
db.authenticate(dbConfig.mongo_api_username,dbConfig.mongo_api_password)

def check_key(key,database,site):
    collection=db.api_keys
    exists=collection.find_one({'key':key})
    ret=False
    if exists:
        if exists["database"]==database:
            for i in exists["access"]:
                if site==i or i=="all":
                    ret = True
                    break
    return ret

def accessed(key):
    collection=db.api_keys
    exists=collection.find_one({'key':key})
    if exists:
        data={"accessed":exists["accessed"]+1,"owner":exists["owner"],"key":exists["key"],"database":exists["database"],"access":exists["access"]}
        collection.update({"_id":exists["_id"]},data)
        
def add_key(owner,database,access):
    #owner should be an email-adress, access a list of accessable sites or all
    salt=random.random()
    key=hashlib.md5()
    key.update(owner+str(salt))
    key=key.hexdigest()
    collection=db.api_keys
    exists=collection.find_one({'owner':owner,"database":database})
    if not exists:
        collection.insert({"key":key,"owner":owner,"accessed":0,"database":database,"access":access})
        return key
    else:
        return False

def get_all():
    collection=db.api_keys
    return collection.find()

def get_key(owner):
    collection=db.api_keys
    exists=collection.find({'owner':owner}) 
    return exists
if __name__=="__main__":
    import sys
  #  try:
    arg=sys.argv[1]
    print arg
    if arg=="add":
        if len(sys.argv)>4:
            owner=sys.argv[2]
            database=sys.argv[3]
            access=sys.argv[4:]
            print access
            print add_key(owner,database,access)
    elif arg=="get":
        if len(sys.argv)>2:
            owner=sys.argv[2]
            keys=get_key(owner)
            for key in keys:
                print "Key",key['key'],"for "+key["database"]
                print "Accessed", key["accessed"], "times"
    elif arg=="all":
        keys=get_all()
        for k in keys:
            print "Owner",k["owner"],"key",k['key'],"database",k['database'] ,",accessed", k["accessed"], "times"
    elif arg=="help":
        print "use add owner to add key, get owner to retrive key,all to see all keys" 
            #    except:
 #       print "Need command-line argument, use help for more information"
