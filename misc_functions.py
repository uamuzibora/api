def insert_mch(aggregate,main_key,text,group_number=None):
    if text in aggregate[main_key].keys():
        if group_number!=None:
            aggregate[main_key][text][group_number]+=1
        else:
            aggregate[main_key][text]+=1
    else:
        if group_number!=None:
            aggregate[main_key][text]=[0,0]
            aggregate[main_key][text][group_number]+=1
        else:
            aggregate[main_key][text]=1

def insert(aggregate,main_key,location,group_number,text=None):
    if group_number<0:
        return False
    if location:
        if text:
            if text in aggregate[main_key].keys():
                if location in aggregate[main_key][text].keys():
                    aggregate[main_key][text][location][group_number]+=1
                else:
                    aggregate[main_key][text][location]=[0,0,0,0]
                    aggregate[main_key][text][location][group_number]+=1
            else:
                aggregate[main_key][text]={location:[0,0,0,0]}
                aggregate[main_key][text][location][group_number]+=1
        else:
            if location in aggregate[main_key].keys():
                aggregate[main_key][location][group_number]+=1
            else:
                aggregate[main_key][location]=[0,0,0,0]
                aggregate[main_key][location][group_number]+=1
    else:
        if text:
            if text in aggregate[main_key].keys():
                aggregate[main_key][text][group_number]+=1
            else:
                aggregate[main_key][text]=[0,0,0,0]
                aggregate[main_key][text][group_number]+=1
        else:
            if len(aggregate[main_key])>1:
                aggregate[main_key][group_number]+=1
            else:
                aggregate[main_key]=[0,0,0,0]


def group(patient):
    group_number=-2
    age_limit=14
    if patient['age']>age_limit:
        group_number=2
    elif patient['age']<=age_limit:
        group_number=0
    
    if patient['sex']=="F":
        group_number+=1
    elif patient["sex"]=="M":
        pass
    else:
        group_number+=-4
    return group_number
def group_mch(patient):
    group_number=-2
    age_limit=1
    if patient['age']>age_limit:
        group_number=2
    elif patient['age']<=age_limit:
        group_number=0
    
    if patient['sex']=="F":
        group_number+=1
    elif patient["sex"]=="M":
        pass
    else:
        group_number+=-4
    return group_number
