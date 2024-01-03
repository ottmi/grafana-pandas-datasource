import dcdb
import re
from grafana_pandas_datasource import create_app
from grafana_pandas_datasource.registry import data_generators as dg
from grafana_pandas_datasource.service import pandas_component


def define_and_register_data():
    _metrics = []

    def load_metrics():
        dcdb.connect('stor')
        nonlocal _metrics
        _metrics = dcdb.getSensors()
        _metrics.sort()
        dcdb.disconnect()
    
    def split_metrics():
        regex = re.compile(r"^/([^/]+)/(.*)/([^/]+)$")
        split_metrics = {}
        for m in _metrics:
            match = regex.match(m)
            if match is not None:
                groups = regex.match(m).groups()
                if len(groups) == 3:
                    if split_metrics.get(groups[0]) is None:
                        split_metrics[groups[0]] = {}
                    if split_metrics[groups[0]].get(groups[1]) is None:
                        split_metrics[groups[0]][groups[1]] = []
                    split_metrics[groups[0]][groups[1]].append(groups[2])
            else:
                print("Error splitting:", m)
        return split_metrics

    def get_metric(target, ts_range):
        dcdb.connect('stor')
        print("dcdbquery:", target, ts_range["$gt"], ts_range["$lte"])
        df = dcdb.query(target, ts_range["$gt"], ts_range["$lte"]).tz_localize("UTC")
        df.rename(columns={'value': target}, inplace=True)
        dcdb.disconnect()
        return df

    def list_metrics(target):
        list = []
        for m in _metrics:
            if target in m:
                list.append(m)
        return list

    load_metrics()
    dg.add_metrics(_metrics)
    dg.add_split_metrics(split_metrics())
    dg.add_metric_reader("$default", get_metric)
    dg.add_metric_finder("$default", list_metrics)

def main():

    # Define and register data generators.
    define_and_register_data()

    # Create Flask application.
    app = create_app()

    # Register pandas component.
    app.register_blueprint(pandas_component, url_prefix="/")

    # Invoke Flask application.
    app.run(host="127.0.0.1", port=3003, debug=True)


if __name__ == "__main__":
    main()
