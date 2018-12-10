
from flask import Flask, request, Response, abort
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os

import json
import pytz
import iso8601
import requests
import logging

app = Flask(__name__)

logger = None

base_url = "https://consumption.azure.com/"

def datetime_format(dt):
    return '%04d' % dt.year + dt.strftime("-%m-%dT%H:%M:%SZ")


def to_transit_datetime(dt_int):
    return "~t" + datetime_format(dt_int)

class DataAccess:
    def __init__(self):
        self._entities = {"balancesummary": [], "usagedetails": [], "marketplacecharges": [], "billingperiods": [], "reservationcharges": [], "reservationdetails": []}

    def get_entities(self, since, datatype, jwt_token, enrollment_number):
        if not datatype in self._entities:
            abort(404)

        return self.get_entitiesdata(datatype, since, jwt_token, enrollment_number)

    def get_entitiesdata(self, datatype, since, jwt_token, enrollment_number):
        # if datatype in self._entities:
        #     if len(self._entities[datatype]) > 0 and self._entities[datatype][0]["_updated"] > "%sZ" % (datetime.now() - timedelta(hours=12)).isoformat():
        #        return self._entities[datatype]

        entities = []

        end = datetime.now(pytz.UTC).date()

        url = "%sv2/enrollments/%s/billingperiods" % (base_url, enrollment_number)
        logger.info("Getting %s entities by %s" % (datatype, url))
        response = requests.get(url, headers={'Authorization': "Bearer %s" % jwt_token})
        periods = response.json()

        period_nr = len(periods) -1
        while period_nr >= 0:

            period_start = iso8601.parse_date(periods[period_nr]["billingStart"]).date()

            if since is None:
                start = period_start
            else:
                start = iso8601.parse_date(since).date()

            if len(entities) == 0:
                end = period_start + relativedelta(months=+2)

            if start <= period_start:
                more = True
                url = ""
                if datatype in ["usagedetails"]:
                    if periods[period_nr]["usageDetails"] > "":
                        url = "%s%s" % (base_url,periods[period_nr]["usageDetails"])
                        logger.info("Getting %s entities - from %s to %s" % (datatype, start, end))
                elif datatype in ["reservationcharges"]:
                    url = "%sv3/enrollments/%s/%sbycustomdate?startTime=%s&endTime=%s" % (base_url,enrollment_number,datatype,start,end)
                    logger.info("Getting %s entities - from %s to %s" % (datatype, start, end))
                else:
                    url = "%sv2/enrollments/%s/%s" % (base_url, enrollment_number, datatype)



                logger.info("Getting %s entities by %s" % (datatype, url))

                while more and url != "":
                    response = requests.get(url, headers={'Authorization': "Bearer %s" % jwt_token})
                    logger.info("Got result code %s" % (response))
                    while response.status_code > 400:
                        logger.info("Retry url: %s for better result..." % (url))
                        response = requests.get(url, headers={'Authorization': "Bearer %s" % jwt_token})
                        logger.info("Got result code %s" % (response))

                    result = response.json()
                    if "nextLink" in result and result["nextLink"] is not None:
                        url = result["nextLink"]
                    else:
                        more = False

                    if datatype in ["usagedetails", "reservationcharges", "reservationdetails"]:

                        if "data" in result:
                            if datatype == "usagedetails":
                                for e in result["data"]:
                                    e.update({"_id": e["meterId"] + "-" + e["date"] + e["instanceId"].replace('/','-')})
                                    e.update({"billingPeriodId": "%s" % periods[period_nr]["billingPeriodId"]})
                                    e.update({"_updated": "%s" % period_start})
                                    if "date" in e:
                                        e.update({"date": "%s" % to_transit_datetime(iso8601.parse_date(e["date"]))})
                                    entities.append(e)
                                    if period_start >= end:
                                        break

                            if datatype == "reservationcharges":
                                for e in result["data"]:
                                    e.update({"_id": e["reservationOrderId"] + "-" + e["eventDate"] + e["eventDate"].replace('/', '-')})
                                    if "eventDate" in e:
                                        e.update({"_updated": "%s" % e["eventDate"]})
                                        e.update({"eventDate": "%s" % to_transit_datetime(iso8601.parse_date(e["eventDate"]))})
                                    entities.append(e)

                            if datatype == "reservationdetails":
                                for e in result["data"]:
                                    e.update({"_id": e["reservationId"] + "-" + e["usageDate"] + e["instanceId"].replace('/', '-')})
                                    if "eventDate" in e:
                                        e.update({"_updated": "%s" % e["eventDate"]})
                                        e.update({"eventDate": "%s" % to_transit_datetime(iso8601.parse_date(e["eventDate"]))})
                                    entities.append(e)

                            logger.info("Gotten %s entities of type %s" % (len(entities), datatype))



                    if datatype == "billingperiods":
                        for e in result:
                            e.update({"_id": "%s-%s" % (enrollment_number, e["billingPeriodId"])})
                            e.update({"_updated": "%s" % e["billingEnd"]})
                            e.update({"billingEnd": "%s" % to_transit_datetime(iso8601.parse_date(e["billingEnd"]))})
                            e.update({"billingStart": "%s" % to_transit_datetime(iso8601.parse_date(e["billingStart"]))})
                            e.update({"balanceSummary": "%s%s" % (base_url,e["balanceSummary"])})
                            e.update({"usageDetails": "%s%s" % (base_url, e["usageDetails"])})
                            e.update({"marketplaceCharges": "%s%s" % (base_url, e["marketplaceCharges"])})
                            e.update({"priceSheet": "%s%s" % (base_url, e["priceSheet"])})
                            #response = requests.get("https://consumption.azure.com/v2/enrollments/68450484/billingperiods/201803/usagedetails", headers={'Authorization': "Bearer %s" % jwt_token})
                            #result = response.json()
                            #e.update({"price": result})
                            entities.append(e)
                        logger.info("Gotten %s entities of type %s" % (len(entities), datatype))

                    if period_start >= end:
                        break

            period_nr -= 1

            if period_start >= end:
                break

        return entities

data_access_layer = DataAccess()

def get_var(var, default = None):
    envvar = default
    if var.upper() in os.environ:
        envvar = os.environ[var.upper()]
    elif request:
        envvar = request.args.get(var)
    logger.debug("Setting %s = %s" % (var, envvar))
    return envvar


@app.route('/<datatype>', methods=['GET'])
def get_entities(datatype):
    since = get_var('since')
    jwt_token = get_var('jwt_token')
    enrollment_number = get_var('enrollment_number')

    ent = data_access_layer.get_entities(since, datatype, jwt_token, enrollment_number)

    #entities = sorted(ent, key=lambda k: k["_updated"])

    return Response(json.dumps(ent), mimetype='application/json')



if __name__ == '__main__':
    # Set up logging
    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger('azure-billing-microservice')

    # Log to stdout
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)

    logger.setLevel(logging.INFO)

    app.run(debug=False, host='0.0.0.0', port=5000)

