from pprint import pprint

import great_expectations as ge
from great_expectations.checkpoint import LegacyCheckpoint


context = ge.data_context.DataContext()

expectation_suite_name = "stepik"

suite = context.get_expectation_suite(expectation_suite_name)

batch_kwargs = {'table': 'stepik_courses', 'schema': None, 'data_asset_name': 'stepik_courses', 'datasource': 'courses', 'bigquery_temp_table': 'SOME_PROJECT.SOME_DATASET.ge_tmp_7c89fc29'}
batch = context.get_batch(batch_kwargs, suite)
results = LegacyCheckpoint(
    name="_temp_checkpoint",
    data_context=context,
    batches=[
        {
            "batch_kwargs": batch_kwargs,
            "expectation_suite_names": [expectation_suite_name]
        }
    ]
).run()
pprint(results)
validation_result_identifier = results.list_validation_result_identifiers()[0]
