import dbConfig
import pymongo
import datetime
from misc_functions import *
import math
import copy

def locations(database="openmrs_aggregation"):
    connection=pymongo.MongoClient()
    db=connection[database]
    db.authenticate(dbConfig.mongo_username,dbConfig.mongo_password)
    collection=db.aggregate
    collection=db.aggregate
    data={}
    locations=[]
    for entry in collection.find():
        for location in entry["enrolled"].keys():
            if location not in locations:
                locations.append(location)
    return locations

    

def patients(database="openmrs_aggregation"):
    connection=pymongo.MongoClient()
    db=connection[database]
    db.authenticate(dbConfig.mongo_username,dbConfig.mongo_password)
    collection=db.patients
    data=[]
    for entry in collection.find():
        del entry["_id"]
        if database=="openmrs_aggregation":
            if "date" in entry.keys() and entry["date"]:
                entry["date"]=entry["date"].isoformat()
            else:
                entry["date"]=""
            if "first_art_start_date" in entry.keys() and entry["first_art_start_date"]:
                    entry["first_art_start_date"]=entry["first_art_start_date"].isoformat()
            else:
                entry["first_art_start_date"]=""
            if "current_regimen_start_date" in entry.keys() and entry["current_regimen_start_date"]:
                entry["current_regimen_start_date"]=entry["current_regimen_start_date"].isoformat()
            else:
                entry["current_regimen_start_date"]=""
            if entry["pregnancy"]:
                tmp=[]
                for i in entry["pregnancy"]:
                    tmp.append(i.isoformat())
                entry["pregnancy"]=tmp
            print entry
        data.append(entry)
    return data

def total_patients(database="openmrs_aggregation"):
    connection=pymongo.MongoClient()
    db=connection[database]
    db.authenticate(dbConfig.mongo_username,dbConfig.mongo_password)
    collection=db.patients
    return collection.count()
    
    

def dashboard(location):
    connection=pymongo.MongoClient()
    db=connection.openmrs_aggregation
    db.authenticate(dbConfig.mongo_username,dbConfig.mongo_password)
    collection=db.aggregate
    data={}
    for entry in collection.find():
        if location=="all":
            data[entry["timestamp"].isoformat()]=entry
        else:
            data[entry["timestamp"].isoformat()]=entry[location]
        del entry["timestamp"]
        del entry["_id"]
    return data

def mch_dashboard(location):
    connection=pymongo.MongoClient()
    db=connection.mch_aggregation
    db.authenticate(dbConfig.mongo_username,dbConfig.mongo_password)
    collection=db.aggregate
    data={}
    for entry in collection.find():
        if location=="all":
            data[entry["timestamp"].isoformat()]=entry
        else:
            data[entry["timestamp"].isoformat()]=entry[location]
        del entry["timestamp"]
        del entry["_id"]
    return data


def performance(database="openmrs_aggregation"):
    connection=pymongo.MongoClient()
    db=connection[database]
    db.authenticate(dbConfig.mongo_username,dbConfig.mongo_password)
    collection=db.performance
    for entry in collection.find():
        data=entry
        del entry["timestamp"]
        del entry["_id"]
    return data

def performance_by_week(first_date,database="openmrs_aggregation"):
    first_date=datetime.datetime.strptime(first_date,"%Y-%m-%d")
    data=performance(database)
    return_data={}
    #get the weeks
    date=first_date
    weeks=[]
    counter=1
    while(date<datetime.datetime.now()):
        weeks.append(["Week "+str(counter),0])
        counter+=1
        date=date+datetime.timedelta(days=7)
    for visit_type in data.keys():
        return_data[visit_type]={}
        for user in data[visit_type].keys():
            dates=[datetime.datetime.strptime(d,"%Y-%m-%d") for d in data[visit_type][user].keys()]
            dates=sorted(dates)
            return_data[visit_type][user]=copy.deepcopy(weeks)
            for date in dates:
                if date>first_date:
                    diff=date-first_date
                    week=int(math.floor(diff.days/7))
                    return_data[visit_type][user][week][1]+=data[visit_type][user][date.strftime("%Y-%m-%d")]
    return return_data

def report(start_date,end_date,location):
    connection=pymongo.MongoClient()
    db=connection.openmrs_aggregation
    db.authenticate(dbConfig.mongo_username,dbConfig.mongo_password)
    collection=db.patients
    data={'patient_source':{},'start_art':{},'enrolled':{},'ever_on_art':{},'currently_on_art':{},'eligible_no_art':{},'on_cotrimoxazole':{}}
    # Go through each patient to compute numbers.
    for p in collection.find():
        #print p
        if (p["location"]==location or location=="all") and "date" in p.keys() and p["date"]:
            
            group_number=group(p)
            if p["date"]>start_date and p["date"]<end_date:
                insert(data,'patient_source',None,group_number,text=p['patient_source'])
            if "first_art_start_date" in p.keys():
                if p["first_art_start_date"]>start_date and p["first_art_start_date"]<end_date:
                    insert(data,'start_art',None,group_number,text=p["who_stage_f"])
            if p["date"]<end_date:
                insert(data,'enrolled',None,group_number)
                if "first_art_start_date" in p.keys() and p["first_art_start_date"]<end_date:
                    insert(data,'ever_on_art',None,group_number)
                if p["on_art"] and "current_regimen_start_date" in p.keys() and p["current_regimen_start_date"]<end_date:                
                    #if pregnant
                    if pregnant(p,end_date):
                        insert(data,'currently_on_art',None,group_number,text="Pregnant")
                    else:
                        insert(data,'currently_on_art',None,group_number,text="All Others")
                if p["eligible_for_art"] and not p["on_art"]:
                    insert(data,"eligible_no_art",None,group_number)
                if p["on_cotrimoxazole"]:
                    insert(data,"on_cotrimoxazole",None,group_number)
    return data

def pregnant(p,end_date):
    if p["pregnancy"]==0:
        return False
    else:
        for date in p["pregnancy"]:
            date_diff=end_date-date
            if date_diff.days<9*30: #9 months
                return True
    return False
if __name__=="__main__":
    import datetime
    print patients()
   # print report(datetime.datetime.now()-datetime.timedelta(days=365*100),datetime.datetime.now(),"all")
#    data=hiv_performance_by_week("1990-10-05")

