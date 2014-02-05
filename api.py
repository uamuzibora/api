from bottle import run,route,request,abort,app
import datetime
import get_data
import api_key
import helper_functions
base_url="/api"

@route(base_url+"/locations/hiv")
def locations_hiv():
    return {"locations":get_data.locations(database="openmrs_aggregation")}
@route(base_url+"/locations/mch")
def locations_hiv():
    return {"locations":get_data.locations(database="mch_aggregation")}

@route(base_url+'/hiv/<req>')
def hiv(req):
    return get_data.dashboard(req)

@route(base_url+'/mch/<req>')
def mch(req):
    return get_data.mch_dashboard(req)

@route(base_url+'/hiv/total_patients')
def hiv_total_patients():
    return {"total_patients":get_data.total_patients(database="openmrs_aggregation")}

@route(base_url+'/mch/total_patients')
def mch_total_patients():
    return {"total_patients":get_data.total_patients(database="mch_aggregation")}

@route(base_url+'/performance/hiv')
def performace_hiv():
    if api_key.check_key(request.query.api_key,"hiv","performace"):
        api_key.accessed(request.query.api_key)
        return get_data.performance(database="openmrs_aggregation")
    else:
        abort(401, "Access denied.")
@route(base_url+'/performance/hiv/<date>')
def performace_hiv(date):
    if api_key.check_key(request.query.api_key,"hiv","performace"):
        api_key.accessed(request.query.api_key)

        if request.query.type=="csv":
            data=get_data.performance_by_week(date,database="openmrs_aggregation")
            return helper_functions.weeks_to_csv(data)
        else:
            return get_data.performance_by_week(date)
    else:
        abort(401, "Access denied.")        

@route(base_url+'/performance/mch')
def performace_mch():
    if api_key.check_key(request.query.api_key,"mch","performance"):
        api_key.accessed(request.query.api_key)
        return get_data.performance(database="mch_aggregation")
    else:
        abort(401, "Access denied.")
@route(base_url+'/performance/mch/<date>')
def performace_mch(date):
    if api_key.check_key(request.query.api_key,"mch","performance"):
        api_key.accessed(request.query.api_key)
        print request.query.type,"csv"
        if request.query.type=="csv":
            data=get_data.performance_by_week(date,database="mch_aggregation")
            return helper_functions.weeks_to_csv(data)

        else:
            return get_data.performance_by_week(date,database="mch_aggregation")
    else:
        abort(401, "Access denied.")        

@route(base_url+'/patients/mch')
def patients_mch():
    if api_key.check_key(request.query.api_key,"mch","patients"):
        api_key.accessed(request.query.api_key)
        if request.query.type=="csv":
            data=get_data.patients(database="mch_aggregation")
            return helper_functions.patients_to_csv(data)

        else:
            return get_data.patients(database="mch_aggregation")
    else:
        abort(401, "Access denied.")

@route(base_url+'/patients/hiv')
def patients_hiv():
    if api_key.check_key(request.query.api_key,"hiv","patients"):
        api_key.accessed(request.query.api_key)
        if request.query.type=="csv":
            data=get_data.patients(database="openmrs_aggregation")
            return helper_functions.patients_to_csv(data)

        else:
            return {'patients':get_data.patients(database="openmrs_aggregation")}
    else:
        abort(401, "Access denied.")
@route(base_url+'/report/hiv/<location>')
def report_hiv(location):

    if request.query.start:
        start=request.query.start
        start=datetime.datetime.strptime(start,"%d/%m/%Y")
    else:
        start=datetime.datetime(1990,1,1)
    if request.query.end:
        end=request.query.end
        end=datetime.datetime.strptime(end,"%d/%m/%Y")
    else:
        end=datetime.datetime.now()
    return get_data.report_hiv(start,end,location)
@route(base_url+'/report/mch/<location>')
def report_hiv(location):
    if request.query.start:
        start=request.query.start
        start=datetime.datetime.strptime(start,"%d/%m/%Y")
    else:
        start=datetime.datetime(1990,1,1)
    if request.query.end:
        end=request.query.end
        end=datetime.datetime.strptime(end,"%d/%m/%Y")
    else:
        end=datetime.datetime.now()
    return get_data.report_mch(start,end,location)
@route(base_url+'/verification/who/hiv')
def verification_who():
    if api_key.check_key(request.query.api_key,"hiv","verification_who"):
        api_key.accessed(request.query.api_key)
        if request.query.type=="csv":
            data=get_data.verification_who(database="openmrs_aggregation")
            return helper_functions.verification_to_csv(data)

        else:
            return get_data.verification_who(database="openmrs_aggregation")
    else:
        abort(401, "Access denied.")
@route(base_url+'/verification/cd4/hiv')
def verification_cd4():
    if api_key.check_key(request.query.api_key,"hiv","verification_cd4"):
        api_key.accessed(request.query.api_key)
        if request.query.type=="csv":
            data=get_data.verification_cd4(database="openmrs_aggregation")
            return helper_functions.verification_to_csv(data)

        else:
            return get_data.verification_cd4(database="openmrs_aggregation")
    else:
        abort(401, "Access denied.")

@route(base_url+'/verification/followup/hiv')
def verification_followup():
    if api_key.check_key(request.query.api_key,"hiv","verification_followup"):
        api_key.accessed(request.query.api_key)
        if request.query.type=="csv":
            data=get_data.verification_followup(database="openmrs_aggregation")
            return helper_functions.verification_to_csv(data)

        else:
            return get_data.verification_followup(database="openmrs_aggregation")
    else:
        abort(401, "Access denied.")

@route(base_url+'/verification/missedappointment/hiv')
def verification_missed_appointment():
    if api_key.check_key(request.query.api_key,"hiv","verification_missed_appointment"):
        api_key.accessed(request.query.api_key)
        if request.query.type=="csv":
            data=get_data.verification_missed_appointment(database="openmrs_aggregation")
            return helper_functions.verification_to_csv(data)

        else:
            return get_data.verification_missed_appointment(database="openmrs_aggregation")
    else:
        abort(401, "Access denied.")

@route(base_url+'/verification/eligible/hiv')
def verification_eligible():
    if api_key.check_key(request.query.api_key,"hiv","verification_eligible"):
        api_key.accessed(request.query.api_key)
        if request.query.type=="csv":
            data=get_data.verification_eligible(database="openmrs_aggregation")
            return helper_functions.verification_to_csv(data)

        else:
            return get_data.verification_eligible(database="openmrs_aggregation")
    else:
        abort(401, "Access denied.")

@route(base_url+'/neel/hiv')
def verification_eligible():
    if api_key.check_key(request.query.api_key,"hiv","neel"):
        api_key.accessed(request.query.api_key)
        if request.query.type=="csv":
            data=get_data.neel(database="openmrs_aggregation")
            return helper_functions.patients_to_csv(data)

        else:
            return get_data.neel(database="openmrs_aggregation")
    else:
        abort(401, "Access denied.")


if __name__=="__main__":
    run(host="localhost",port="8080",debug=True)
else:
    application = app()

