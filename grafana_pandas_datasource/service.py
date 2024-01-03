"""
Copyright 2017 Linar <linar@jether-energy.com>
Copyright 2020-2022 Andreas Motl <andreas.motl@panodata.org>

License: GNU Affero General Public License, Version 3
"""
import pandas as pd
import json
from flask import Blueprint, abort, current_app, jsonify, request
from flask_cors import cross_origin

from grafana_pandas_datasource.registry import data_generators as dg
from grafana_pandas_datasource.core import (
    annotations_to_response,
    dataframe_to_json_table,
    dataframe_to_response,
)

pandas_component = Blueprint("pandas-component", __name__)
methods = ("GET", "POST")


@pandas_component.route("/", methods=methods)
@cross_origin()
def test_datasource():
    current_app.logger.info('Request to "test_datasource" endpoint at /')
    return (
        "Grafana pandas datasource: Serve NumPy data via pandas data frames to Grafana. "
        'For documentation, see <a href="https://github.com/panodata/grafana-pandas-datasource">https://github.com/panodata/grafana-pandas-datasource</a>.'
    )

@pandas_component.route("/search", methods=methods)
@cross_origin()
def search_metrics():
    current_app.logger.info('Request to "search_metrics" endpoint at /search')
    req = request.get_json()
    print(json.dumps(req, indent=4))
    target = req.get("target", "*")
    response = []
    if (len(target)) >= 3:
        response = list(dg.metric_finders['$default'](target))
    return jsonify(response)

@pandas_component.route("/metrics", methods=methods)
@cross_origin()
def find_metrics():
    current_app.logger.info('Request to "find_metrics" endpoint at /metrics')
    req = request.get_json()
    print(json.dumps(req, indent=4))

    current_metric = req.get("metric")
    payload = req.get("payload")
    domain = None
    if payload is not None:
        domain = payload.get("domain", None)
    payloads = [{'name': 'domain', 'label': 'Domain', 'type': 'input', 'reloadMetric': True}, {'name': 'location', 'label': 'Location', 'type': 'input', 'reloadMetric': True}]
    if current_metric is None:
        res = [{'value': 'DescribeMetricList', 'label': 'Describe Metric below', 'payloads': payloads}]
    else:
        res = [{'value': current_metric, 'payloads': payloads}]

    if domain is not None:
        location = payload.get("location")
        if location is not None:
            for m in dg.split_metrics[domain][location]:
                full_metric = "/" + domain + "/" + location + "/" + m
                if full_metric != current_metric:
                    res.append({'value': full_metric, 'payloads': payloads})
    print(res)
    return jsonify(res)

@pandas_component.route("/metric-payload-options", methods=methods)
@cross_origin(max_age=600)
def payload_options():
    current_app.logger.info('Request to "payload_options" endpoint at /metric-payload-options')
    req = request.get_json()
    print(json.dumps(req, indent=4))
    response = []
    if req['name'] == 'namespace':
        for ns in dg.split_metrics.keys():
            response.append({'name': ns})
        return jsonify(response)
    elif req['name'] == 'location':
        payload = req.get("payload")
        ns = None
        if payload is not None:
            ns = payload.get("namespace", "beast")
        for l in dg.split_metrics[ns].keys():
            response.append({'name': ns})
        return jsonify(response)
    
    return ""

@pandas_component.route("/tag-keys", methods=methods)
@cross_origin(max_age=600)
def tagkeys():
    current_app.logger.info('Request to "tagkeys" endpoint at /tag-keys')
    req = request.get_json(silent=True)
    print(type(req))
    print(json.dumps(req, indent=4))
    res = [{'type': 'string', 'text': 'Domain'}]
    print(res)
    return jsonify(res)

@pandas_component.route("/tag-values", methods=methods)
@cross_origin(max_age=600)
def tagvalues():
    current_app.logger.info('Request to "tagvalues" endpoint at /tag-values')
    req = request.get_json()
    print(json.dumps(req, indent=4))
    response = []
    if req['key'] == 'Domain':
        for ns in dg.metric_finders.keys():
            response.append({'text': ns})
        return jsonify(response)
    else: 
        return ""

@pandas_component.route("/query", methods=methods)
@cross_origin(max_age=600)
def query_metrics():
    current_app.logger.info('Request to "query_metrics" endpoint at /query')
    req = request.get_json()
    print(json.dumps(req, indent=4))

    results = []

    ts_range = {
        "$gt": pd.Timestamp(req["range"]["from"]).to_pydatetime(),
        "$lte": pd.Timestamp(req["range"]["to"]).to_pydatetime(),
    }

    if "intervalMs" in req:
        freq = str(req.get("intervalMs")) + "ms"
    else:
        freq = None

    for target in req["targets"]:
        req_type = target.get("type", "timeserie")
        target = target["target"]
        if ":" in target:
            finder, target = target.split(":", 1)
        else:
            finder = '$default'

        query_results = dg.metric_readers[finder](target, ts_range)

        if req_type == "table":
            results.extend(dataframe_to_json_table(target, query_results))
        else:
            results.extend(dataframe_to_response(target, query_results, freq=freq))
    
    return jsonify(results)


@pandas_component.route("/annotations", methods=methods)
@cross_origin(max_age=600)
def query_annotations():
    current_app.logger.info('Request to "query_annotations" endpoint at /annotations')
    req = request.get_json()

    results = []

    ts_range = {
        "$gt": pd.Timestamp(req["range"]["from"]).to_pydatetime(),
        "$lte": pd.Timestamp(req["range"]["to"]).to_pydatetime(),
    }

    query = req["annotation"]["query"]

    if ":" not in query:
        abort(404, Exception("Target must be of type: <finder>:<metric_query>, got instead: " + query))

    finder, target = query.split(":", 1)
    results.extend(annotations_to_response(query, dg.annotation_readers[finder](target, ts_range)))

    return jsonify(results)


@pandas_component.route("/panels", methods=methods)
@cross_origin()
def get_panel():
    current_app.logger.info('Request to "get_panel" endpoint at /panels')
    req = request.args

    ts_range = {
        "$gt": pd.Timestamp(int(req["from"]), unit="ms").to_pydatetime(),
        "$lte": pd.Timestamp(int(req["to"]), unit="ms").to_pydatetime(),
    }

    query = req["query"]

    if ":" not in query:
        abort(404, Exception("Target must be of type: <finder>:<metric_query>, got instead: " + query))

    finder, target = query.split(":", 1)
    return dg.panel_readers[finder](target, ts_range)
