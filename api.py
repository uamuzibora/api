from bottle import run,route,request,abort,app
import datetime
import get_data
import api_key
import helper_functions
base_url="/api"

@route(base_url+"/locations/hiv")
def locations_hiv():
    return {"locations":get_data.locations(database="openmrs_aggregation")}

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
    if api_key.check_key(request.query.api_key,"hiv"):
        api_key.accessed(request.query.api_key)
        return get_data.performance(database="openmrs_aggregation")
    else:
        abort(401, "Access denied.")
@route(base_url+'/performance/hiv/<date>')
def performace_hiv(date):
    if api_key.check_key(request.query.api_key,"hiv"):
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
    if api_key.check_key(request.query.api_key,"mch"):
        api_key.accessed(request.query.api_key)
        return get_data.performance(database="mch_aggregation")
    else:
        abort(401, "Access denied.")
@route(base_url+'/performance/mch/<date>')
def performace_mch(date):
    if api_key.check_key(request.query.api_key,"mch"):
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
    if api_key.check_key(request.query.api_key,"mch"):
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
    if api_key.check_key(request.query.api_key,"hiv"):
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
    return get_data.report(start,end,location)





if __name__=="__main__":
    run(host="localhost",port="8080",debug=True)
else:
    application = app()

