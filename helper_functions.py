import StringIO

def weeks_to_csv(data):
    output=StringIO.StringIO()
    for form_type in data.keys():
        output.write(form_type+"\n")
        weeks=data[form_type][data[form_type].keys()[0]]
        for w in weeks:
            output.write(","+w[0])
        for user in data[form_type].keys():
            output.write("\n"+user)
            for w in data[form_type][user]:
                output.write(","+str(w[1]))
        output.write("\n")
        output.write("\n")
    return output.getvalue()

def patients_to_csv(data):
    output=StringIO.StringIO()
    headers=sorted(data[0].keys())
    for h in headers:
        if h=="cd4_count":
            output.write("first cd4,last cd4,mean cd4,regression cd4,")
        else:
            output.write(h+",")
    for patient in data:
        output.write("\n")
        for h in headers:
            if h=="cd4_count":
                output.write(str(patient["cd4_count"]["First"])+","+str(patient["cd4_count"]["Last"])+","+str(patient["cd4_count"]["Mean"])+","+str(patient["cd4_count"]["Regression"])+",")
            elif h=="pregnancy":
                if patient[h]==0:
                    output.write(str(patient[h])+",")
                else:
                    for p in patient[h]:
                        output.write(p+"&")
                    output.write(",")
            else:
                if h in patient.keys():
                    output.write(str(patient[h])+",")
                else:
                    output.write(",")
    return output.getvalue()
    
