import os

import dateutil
import pytz
import requests
from flask import Flask, request, Response
import cherrypy
from datetime import datetime, timedelta
import json
import logging
import paste.translogger
import pandas as pd
from urllib.parse import urlencode

app = Flask(__name__)

logger = logging.getLogger("datasource-service")

start = datetime(2019, 1, 1)

base_url = "https://public-api-prod-api.myportal.cloud/"

max_attempts = int(os.environ.get("MAX_ATTEMPTS", "10"))


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


def yield_monthly_consumption(since, api_key, licenses):
    count = 0

    end = datetime.now(pytz.UTC)

    since = since.replace(day=1)
    periods = (end.month - since.month) + 1  # len(result["data"]) -1
    periods += (end.year - since.year) * 12

    logger.info(f"Got {periods} periods")

    for period_nr in range(0,periods):

        for license in licenses:

            logger.info(f"Processing period {period_nr}: {since.strftime('%Y-%m-%d')} for {license}")
            results = get_single_month_consumption(license, since, api_key)
            count += len(results)
            for entity in results:
                yield entity
            logger.info("Yielded %s entities" % count)

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
        response = requests.get(base_url + "/api/licenses", headers={'apikey': api_key})

        logger.debug("Got license result: %s" % (response.json()))

        result = response.json()
        if "data" in result:
            license = []

            for l in result["data"]["licenses"]:
                if l["service_ref"] == "MICROSOFT":
                    logger.info(f"Prepare to prosess license {l['license_id']}")
                    license.append(l["license_id"])
    else:
        license = [license]

    return Response(stream_as_json(yield_monthly_consumption(since, api_key, license)), mimetype='application/json')


def get_single_month_consumption(license_id, since, api_key):
    headers = {
        "Vendor Ressource SKU",
        "Vendor Product Name",
        "Vendor Meter Category",
        "Vendor Meter Sub-Category",
        "Resource Group",
        "UOM",
        "Country currency code",
        "Level Chargeable Quantity",
        "Region",
        "Resource Name",
        "Country customer unit",
        "Vendor Billing Start Date",
        "Vendor Billing End Date",
        "Cost Center",
        "Project",
        "Environment",
        "Application",
        "Custom Tag",
        "Name",
        "Usage Start date"
    }
    params = {"columns[%s]" % ind: header for ind, header in enumerate(headers)}
    month = since.strftime("%Y-%m")
    response = fetch_consumption(api_key, license_id, month, params)

    #   {
    #     "Vendor Ressource SKU": "ed8a651a-e0a3-4de6-a8ae-3b4ce8cb72cf",
    #     "Vendor Product Name": "LRS Data Stored",
    #     "Vendor Meter Category": "Storage",
    #     "Vendor Meter Sub-Category": "Files",
    #     "Resource Group": "subscription-853619d1",
    #     "UOM": "1 GB/Month",
    #     "Country currency code": "NOK",
    #     "Level Chargeable Quantity": 0.3072,
    #     "Region": "northeurope",
    #     "Resource Name": "subscription853619d1",
    #     "Country customer unit": 0.4868464,
    #     "Vendor Billing Start Date": "2020-10-28T00:00:00.000Z",
    #     "Vendor Billing End Date": "2020-11-27T00:00:00.  000Z",
    #     "Cost Center": "",
    #     "Project": "",
    #     "Environment": "",
    #     "Application": "",
    #     "Custom Tag": "",
    #     "Name": "subscription853619d1",
    #     "Usage Start date": "2020-10-30T00:00:00.000Z"
    #   }

    df = pd.DataFrame(columns=response["data"]["headers"], data=response["data"]["lines"])
    if df.empty:
        return []
    # Resource Group can vary in case within a month :(
    df['Resource Group'] = df['Resource Group'].str.lower()
    index = [
        'Vendor Ressource SKU',
        'Vendor Product Name',
        'Vendor Meter Category',
        'Vendor Meter Sub-Category',
        'Resource Group',
        'UOM',
        'Country currency code',
        'Region',
        'Resource Name',
        'Vendor Billing Start Date',
        'Vendor Billing End Date',
        'Cost Center',
        'Project',
        'Environment',
        'Application',
        'Custom Tag',
        'Name'
    ]
    pivot = df.pivot_table(index=index, values=['Level Chargeable Quantity', 'Country customer unit'],
                           aggfunc={'Level Chargeable Quantity': 'sum', 'Country customer unit': 'mean'})
    # TODO see if we can just construct it how we want from the DF instead
    result = json.loads(pivot.to_json(orient='table'))["data"]
    return [dict(r, **{
        # TODO should this be done here or just be constructed in the pipe that reads in this data?
        '_id': "%s_%s_%s_%s_%s"
               % (license_id, r["Resource Group"], r["Resource Name"], month, r["Vendor Ressource SKU"]),
        '_updated': since.strftime("%Y-%m-%dT%H:%M:%SZ"),
        'license': license_id,
        'period': month
    }) for r in result]


def fetch_consumption(api_key, license_id, month, params):
    result = None
    next_page = '/consumption/license/%s?month=%s&noGroup=1&page=1&per_page=5000&%s' % (license_id, month,
                                                                                        urlencode(params))
    while next_page:
        response = requests.get(base_url + next_page, headers={'apikey': api_key}).json()
        if not result:
            # first page
            result = response
        else:
            # add the lines from the additional pages
            result["data"]["lines"] += response["data"]["lines"]
        next_page = response["pagination"].get("next")
    return result


if __name__ == '__main__':
    format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Log to stdout, change to or add a (Rotating)FileHandler to log to a file
    stdout_handler = logging.StreamHandler()
    stdout_handler.setFormatter(logging.Formatter(format_string))
    logger.addHandler(stdout_handler)

    # Comment these two lines if you don't want access request logging
    app.wsgi_app = paste.translogger.TransLogger(app.wsgi_app, logger_name=logger.name,
                                                 setup_console_handler=False)
    app.logger.addHandler(stdout_handler)

    logger.propagate = False
    logger.setLevel(logging.INFO)

    cherrypy.tree.graft(app, '/')

    # Set the configuration of the web server to production mode
    cherrypy.config.update({
        'environment': 'production',
        'engine.autoreload_on': False,
        'log.screen': True,
        'server.socket_port': 5000,
        'server.socket_host': '0.0.0.0'
    })

    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()