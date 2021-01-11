
from flask import Flask, request, Response, abort
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os

import json
import pytz
import iso8601
import requests
import logging
import dateutil.parser

app = Flask(__name__)

logger = None

start = datetime(2019,1,1)

base_url = "https://xsp.arrow.com/index.php"

def datetime_format(dt):
    return '%04d' % dt.year + dt.strftime("-%m-%dT%H:%M:%SZ")

def stream_as_json(generator_function):
    """
    Stream list of objects as JSON array
    :param generator_function:
    :return:
    """
    first = True

    yield '['

    for item in generator_function:
        if not first:
            yield ','
        else:
            first = False

        yield json.dumps(item)

    yield ']'


def add_one_month(t):
    """Return a `datetime.date` or `datetime.datetime` (as given) that is
    one month earlier.

    Note that the resultant day of the month might change if the following
    month has fewer days:

        >>> add_one_month(datetime.date(2010, 1, 31))
        datetime.date(2010, 2, 28)
    """
    import datetime
    one_day = datetime.timedelta(days=1)
    one_month_later = t + one_day
    while one_month_later.month == t.month:  # advance to start of next month
        one_month_later += one_day
    target_month = one_month_later.month
    while one_month_later.day < t.day:  # advance to appropriate day
        one_month_later += one_day
        if one_month_later.month != target_month:  # gone too far
            one_month_later -= one_day
            break
    return one_month_later

def to_transit_datetime(dt_int):
    return "~t" + datetime_format(dt_int)


def get_entitiesdata(datatype, since, api_key, licenses):
    # if datatype in self._entities:
    #     if len(self._entities[datatype]) > 0 and self._entities[datatype][0]["_updated"] > "%sZ" % (datetime.now() - timedelta(hours=12)).isoformat():
    #        return self._entities[datatype]

    count = 0

    end = datetime.now(pytz.UTC)

    if datatype in ["dailyResources"]:
        periods = (end - since).days # len(result["data"]) -1
    else:
        since = since.replace(day = 1)
        periods = (end.month - since.month) + 1  # len(result["data"]) -1
        periods += (end.year - since.year) * 12

    logger.info(f"Got {periods} periods")

    for period_nr in range(0,periods):

        for license in licenses:

            logger.info(f"Processing period {period_nr}: {(since).strftime('%Y-%m-%d')} for {license}")
            more = True

            if datatype in ["dailyResources"]:
                date = (since).strftime("%Y-%m-%d")
                url = "%s/api/consumption/license/%s/azure/%s?beginDay=%s&endDay=%s" % (
                base_url, license, datatype, date, date)
            else:
                date = (since).strftime("%Y-%m")
                url = "%s/api/consumption/license/%s/azure/%s?month=%s" % (base_url, license, datatype, date)

            logger.debug("Getting %s entities by %s" % (datatype, url))

            while more and url != "":

                response = requests.get(url, headers={'apikey': api_key})

                logger.debug("Got result: %s" % (response.json()))

                result = response.json()

                if "pagination" in result and "next" in result["pagination"] and result["pagination"]["next"] is not None:
                    url = base_url + result["pagination"]["next"]
                else:
                    more = False

                if "data" in result:
                    if datatype in ["dailyResources", "resources"]:
                        for e in result["data"]:
                            e.update({"_id": e["resourceId"] + "-" + date})
                            if "resourceGroup" in e and e["resourceGroup"]:
                                e.update({"_id": e["_id"] + "-" + e["resourceGroup"].replace('/', '-')})
                            if "location" in e and e["location"]:
                                e.update({"_id": e["_id"] + "-" + e["location"].replace('/', '-')})
                            if "partnerRef" in e and e["partnerRef"]:
                                e.update({"_id": e["_id"] + "-" + e["partnerRef"].replace('/', '-')})
                            e.update({"period": date})
                            e.update({"license": "%s" % license})
                            e.update({"_updated": "%s" % since.strftime("%Y-%m-%dT%H:%M:%SZ")})
                            if "date" in e:
                                e.update({"date": "%s" % to_transit_datetime(since)})
                            yield e
                            count += 1;


                        logger.info("Gotten %s entities of type %s" % (count, datatype))

        if datatype in ["dailyResources"]:
            since = since + timedelta(days=1)
        else:
            since = add_one_month(since)

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
    since = dateutil.parser.parse(get_var('since') or "2019-03-01T00:00:00.00000Z")
    api_key = get_var('api_key')
    license = get_var('license')

    logger.info(f"Get data from {since}")

    if not license:
        response = requests.get(base_url + "/api/licenses" , headers={'apikey': api_key})

        logger.debug("Got lisence result: %s" % (response.json()))

        result = response.json()
        if "data" in result:
            license = []

            for l in result["data"]["licenses"]:
                if l["service_ref"] == "MICROSOFT":
                    logger.info(f"Prepare to prosess license {l['license_id']}")
                    license.append(l["license_id"])
    else:
        license = [license]


    return Response(stream_as_json(get_entitiesdata(datatype, since, api_key, license)), mimetype='application/json')



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

