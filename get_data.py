import dbConfig
import pymongo
import datetime
from misc_functions import *
import helper_functions
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
    key="enrolled"
    if database=="mch_aggregation":
        key="mothers"

    for entry in collection.find():
        for location in entry[key].keys():
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
            if "next_appointment" in entry.keys() and entry["next_appointment"] and entry["next_appointment"] !="Missing":
                entry["next_appointment"]=entry["next_appointment"].isoformat()
            else:
                entry["next_appointment"]=""
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
            if entry["edd"]:
                tmp=[]
                for i in entry["edd"]:
                    if i!="Missing":
                        tmp.append(i.isoformat())
                    else:
                        tmp.append("Missing")
                entry["edd"]=tmp
        data.append(entry)

    return {i:j for i,j in enumerate(data)}


def neel(database="openmrs_aggregation"):
    connection=pymongo.MongoClient()
    db=connection[database]
    db.authenticate(dbConfig.mongo_username,dbConfig.mongo_password)
    collection=db.patients
    data=[]
    for entry in collection.find():
        if entry["location"] in ["Kakamega PGH MCH","Vihiga DH MCH"]:
            del entry["_id"]
            if database=="openmrs_aggregation":
                if "date" in entry.keys() and entry["date"]:
                    entry["date"]=entry["date"].isoformat()
                else:
                    entry["date"]=""
                if "next_appointment" in entry.keys() and entry["next_appointment"] and entry["next_appointment"] !="Missing":
                    entry["next_appointment"]=entry["next_appointment"].isoformat()
                else:
                    entry["next_appointment"]=""
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
                if entry["edd"]:
                    tmp=[]
                    for i in entry["edd"]:
                        if i!="Missing":
                            tmp.append(i.isoformat())
                        else:
                            tmp.append("Missing")
                        entry["edd"]=tmp
                for key in entry.keys():
                    if key not in ["pid","location","age","sex","hiv_positive_date","patient_source","eligible_for_art","art_eligible_date","cd4_present","cd4_count","who_stage_f","who_stage_l","on_art","first_art_start_date","current_regimen_start_date","regimen_sum","on_cotrimoxazole","date","next_appointment","pregnancy","edd"]:
                        del entry[key]
                    
                data.append(entry)

    return {i:j for i,j in enumerate(data)}


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

def report_hiv(start_date,end_date,location):
    connection=pymongo.MongoClient()
    db=connection.openmrs_aggregation
    db.authenticate(dbConfig.mongo_username,dbConfig.mongo_password)
    collection=db.patients
    data={'patient_source':{},'art_who':{},'on_cd4':{},"eligible_no_art":{},'exiting_care':{},'missing_data':{}}
    # Go through each patient to compute numbers.
    for p in collection.find():
        #print p
        if (p["location"]==location or location=="all") and "date" in p.keys() and p["date"]:
            group_number=group(p)
            if p["date"]>start_date and p["date"]<end_date:
                insert(data,'patient_source',None,group_number,text=p['patient_source'])
                if p["on_art"]:
                    insert(data,'art_who',None,group_number,text=p["who_stage_f"])
                first_cd4=p["cd4_count"]["First"]
                missing=0
                if first_cd4:
                    if first_cd4>350:
                        insert(data,'on_cd4',None,group_number,text=">350")
                    if first_cd4<350:
                        insert(data,'on_cd4',None,group_number,text="<350")
                else:
                    insert(data,'on_cd4',None,group_number,text="Missing")
                    insert(data,"missing_data",None,group_number,text="First CD4 Count")             
                if p["eligible_for_art"] and not p["on_art"]:
                    insert(data,"eligible_no_art",None,group_number)             
                if "inactive_reason" in p and p["inactive_reason"]:
                    insert(data,'exiting_care',None,group_number,text=p["inactive_reason"])
                if "who_stage_f" not in p or p["who_stage_f"]=="Missing":
                    insert(data,"missing_data",None,group_number,text="First WHO Stage")
                if "hiv_positive_date" not in p or p["hiv_positive_date"]==0:
                    insert(data,"missing_data",None,group_number,text="HIV Positive Date")
                if "art_eligible_date" not in p or p["art_eligible_date"]==0:
                    insert(data,"missing_data",None,group_number,text="ART Eligible Date")
    return data
def report_mch(start_date,end_date,location):
    connection=pymongo.MongoClient()
    db=connection.mch_aggregation
    db.authenticate(dbConfig.mongo_username,dbConfig.mongo_password)
    collection=db.patients
    data={'women_enrolled':[0,0],'children_enrolled':[0,0,0,0],'women_hiv':[0,0],"women_art":[0,0],'women_malaria':[0,0],'missing_data':{}}
    # Go through each patient to compute numbers.

    i=0
    for p in collection.find():

        print i

 #print p
        if (p["location"]==location or location=="all") and "date" in p.keys() and p["date"]:
            i+=1
            group_number=group(p)

            if p["date"]>start_date and p["date"]<end_date:

                if p["age"]>5:#Women
                    if p["age"]>18:
                        group_number=1
                    else:
                        group_number=0

                    data['women_enrolled'][group_number]+=1
                    if p["hiv"]==703:
                        data['women_hiv'][group_number]+=1
                    elif p["hiv"]==1118 or p["hiv"]=="Missing":
                        insert_mch(data,'missing_data',"Screened for HIV",group_number=group_number)
                    if p["mother_art"]==1:
                        data['women_art'][group_number]+=1
                    if p["IPT1"]==1 and p["IPT2"]==1 and p["IPT3"]==1:
                        data['women_malaria'][group_number]+=1
                    if p["tb"]==6103 or p["tb"]=="Missing":
                        insert_mch(data,'missing_data',"Screened for TB",group_number=group_number)
                    if p["hypertension"]=="Missing":
                        insert_mch(data,'missing_data',"History of high BP",group_number=group_number)
                    if p["malaria"]==1118 or p["malaria"]=="Missing":
                        insert_mch(data,'missing_data',"Screened for Malaria",group_number=group_number)
                else:
                    data['children_enrolled'][group_mch(p)]+=1
    return data

def verification_who(database="openmrs_aggregation"):
    connection=pymongo.MongoClient()
    db=connection[database]
    db.authenticate(dbConfig.mongo_username,dbConfig.mongo_password)
    collection=db.patients
    data={}
    for entry in collection.find():
        if entry["who_stage_f"]=="Missing":
            location=entry["location"]
            if location in data.keys():
                data[location].append(int(entry["pid"]))
            else:
                data[location]=[int(entry["pid"])]
    return data
def verification_cd4(database="openmrs_aggregation"):
    connection=pymongo.MongoClient()
    db=connection[database]
    db.authenticate(dbConfig.mongo_username,dbConfig.mongo_password)
    collection=db.patients
    data={}
    for entry in collection.find():
        if entry["cd4_count"]["First"]=="Missing" or entry["cd4_count"]["First"]==None :
            location=entry["location"]
            if location in data.keys():
                data[location].append(int(entry["pid"]))
            else:
                data[location]=[int(entry["pid"])]
    return data
def verification_followup(database="openmrs_aggregation"):
    connection=pymongo.MongoClient()
    db=connection[database]
    db.authenticate(dbConfig.mongo_username,dbConfig.mongo_password)
    collection=db.patients
    data={}
    for entry in collection.find():
        if entry["inactive_reason"]=="LOST TO FOLLOWUP":
            location=entry["location"]
            if location in data.keys():
                data[location].append(int(entry["pid"]))
            else:
                data[location]=[int(entry["pid"])]
    return data
def verification_missed_appointment(database="openmrs_aggregation"):
    connection=pymongo.MongoClient()
    db=connection[database]
    db.authenticate(dbConfig.mongo_username,dbConfig.mongo_password)
    collection=db.patients
    data={}
    today=datetime.datetime.now()
    for entry in collection.find():
        if type(entry["next_appointment"])==datetime.datetime:
            if (today-entry["next_appointment"]).days>14:
                location=entry["location"]
                if location in data.keys():
                    data[location].append(int(entry["pid"]))
                else:
                    data[location]=[int(entry["pid"])]
    return data
def verification_eligible(database="openmrs_aggregation"):
    connection=pymongo.MongoClient()
    db=connection[database]
    db.authenticate(dbConfig.mongo_username,dbConfig.mongo_password)
    collection=db.patients
    data={}
    today=datetime.datetime.now()
    for entry in collection.find():
        if entry["eligible_for_art"]==1 and entry["on_art"]==0:
            location=entry["location"]
            if location in data.keys():
                data[location].append(int(entry["pid"]))
            else:
                data[location]=[int(entry["pid"])]
    return data
if __name__=="__main__":
    import datetime
    print helper_functions.patients_to_csv(neel())

#    print report_hiv(datetime.datetime.now()-datetime.timedelta(days=365*100),datetime.datetime.now(),"all")
#    data=hiv_performance_by_week("1990-10-05")

