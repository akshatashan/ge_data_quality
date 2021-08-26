import json
import os
import logging
import requests
import time
import great_expectations as ge
from ruamel.yaml import YAML

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
t0 = time.time()

def process_check(ge_root_dir, date, dataset, table, expectation_suite_name):
    context = ge.data_context.DataContext(context_root_dir=ge_root_dir)
    create_checkpoint(context, date, dataset, table, expectation_suite_name,)
    results = context.run_checkpoint(checkpoint_name=f"{date}_{dataset}_{table}_{expectation_suite_name}")
    logging.info(results)
    if not results.success:
        statistics = results.run_results[list(results.run_results)[0]]['validation_result']['statistics']
        webhook_url = os.environ.get('SLACK_WEBHOOK', None)
        try:
            data = f"Data quality checks in {expectation_suite_name} failed for {dataset}.{table} for {date} \n. " \
                   f"Results: {json.dumps(statistics)}"
            response = requests.post(webhook_url, data=json.dumps({"text": data}),
                                     headers={'Content-Type': 'application/json'})
            if not response.status_code == 200:
                logging.error("error sending slack message")
        except:
            raise



def airflow_process_check(templates_dict, *args, **kwargs):
    ge_root_dir = templates_dict['ge_root_dir']
    date = templates_dict['date']
    dataset = templates_dict['dataset']
    table = templates_dict['table']
    expectation_suite_name = templates_dict['expectation_suite_name']
    process_check(ge_root_dir, date, dataset, table, expectation_suite_name)


def create_checkpoint(context, date, dataset, table, expectation_suite_name):
    yaml = YAML()
    checkpoint_name = f"{date}_{dataset}_{table}_{expectation_suite_name}"
    yaml_config = f"""
        name: {checkpoint_name}
        config_version: 1.0
        class_name: Checkpoint
        run_name_template: {checkpoint_name}
        expectation_suite_name: {expectation_suite_name}
        validations:
          - batch_request:
              datasource_name: akshata-project
              data_connector_name: default_runtime_data_connector_name
              data_asset_name: {dataset}.{table}
              batch_identifiers:
                default_identifier_name: {checkpoint_name}
              runtime_parameters:
                query: SELECT * FROM {dataset}.{table} where date(header.timestamp) = '{date}'
            expectation_suite_name: {expectation_suite_name}
        action_list:
          - name: store_validation_result
            action:
              class_name: StoreValidationResultAction
          - name: store_evaluation_params
            action:
              class_name: StoreEvaluationParametersAction
        """
    context.add_checkpoint(**yaml.load(yaml_config))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Get parameters for data quality checkes')
    parser.add_argument('-ge_root_dir', '--ge_root_dir', type=str, help='root directory of the library')
    parser.add_argument('-date', '--dt', type=str, help='date')
    parser.add_argument('-dataset', '--dataset', type=str, help='import from a given dataset')
    parser.add_argument('-table', '--table', type=str, help='import from a given table')
    parser.add_argument('-expectation_suite_name', '--expectation_suite_name',
                        type=str, help='the name of the expectation suite')
    args = parser.parse_args()
    process_check(args.ge_root_dir, args.dt, args.dataset, args.table, args.expectation_suite_name)
