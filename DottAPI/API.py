import flask
from flask import request, jsonify
import sqlite3

app = flask.Flask(__name__)
app.config["DEBUG"] = True

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def get_last_deployment(filter):
    query = """SELECT deployment_id, total_rides FROM cycles WHERE """
    query += filter
    query +=   " ORDER BY time_cycle_started DESC LIMIT 1;"
    return query

def get_rides(filter):
    query = """SELECT ride_id,time_ride_started,distance,gross_amount,start_latitude,end_latitude,start_longitude,end_longitude FROM rides WHERE deployment_id = """
    query += "'" + filter + "'"
    query += " ORDER BY time_ride_started DESC LIMIT 5;"
    return query

def get_deployments(filter):
    query = """SELECT deployment_id,time_deployment_created,time_cycle_started,total_distance,total_amount from cycles WHERE """
    query += filter
    query += " ORDER BY time_cycle_started DESC LIMIT ?;"
    return query







@app.route('/', methods=['GET'])
def home():
    return '''<h1>Dott rides and deployment cycles API</h1>
<p>API for retreive data from rides and cycles.</p>'''

@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404


@app.route('/vehicles/', methods=['GET'])
def api_filter():
    query_parameters = request.args

    vehicle = query_parameters.get('vehicle')
    qrcode = query_parameters.get('qrcode')
    if not (vehicle or qrcode):
        return page_not_found(404)
    filter = ''

    query_parameters = []
    if vehicle:
        filter += ' vehicle_id=? AND'
        query_parameters.append(vehicle)
    if qrcode:
        filter += ' qr_code=? AND'
        query_parameters.append(qrcode)

    filter = filter[:-4]

    conn = sqlite3.connect('dott.db')
    conn.row_factory = dict_factory
    cur = conn.cursor()

    results_last_dep = cur.execute(get_last_deployment(filter), query_parameters).fetchall()
    if len(results_last_dep) == 0:
        conn.close()
        return page_not_found(404)

    lastdep = results_last_dep[0].get("deployment_id")
    number_rides = int(results_last_dep[0].get("total_rides"))

    if number_rides > 0:
        resultsrides = cur.execute(get_rides(lastdep)).fetchall()
        if number_rides >= 5:
            conn.close()
            return jsonify(resultsrides)

    query_parameters.append(5 - number_rides)
    results_dep = cur.execute(get_deployments(filter),query_parameters).fetchall()
    if number_rides == 0:
        conn.close()
        return jsonify(results_dep)
    else:
        for dep in results_dep:
            resultsrides.append(dep)

    conn.close()
    return jsonify(resultsrides)

app.run()