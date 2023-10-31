# Copyright 2021 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from kfp import components
from kfp import dsl
from kfp.v2 import compiler

chicago_taxi_dataset_op = components.load_component_from_url(
    'https://raw.githubusercontent.com/kubeflow/pipelines/60a2612541ec08c6a85c237d2ec7525b12543a43/components/datasets/Chicago_Taxi_Trips/component.yaml'
)
convert_csv_to_apache_parquet_op = components.load_component_from_url(
    'https://raw.githubusercontent.com/kubeflow/pipelines/0d7d6f41c92bdc05c2825232afe2b47e5cb6c4b3/components/_converters/ApacheParquet/from_CSV/component.yaml'
)
xgboost_train_on_csv_op = components.load_component_from_url(
    'https://raw.githubusercontent.com/kubeflow/pipelines/567c04c51ff00a1ee525b3458425b17adbe3df61/components/XGBoost/Train/component.yaml'
)
xgboost_predict_on_csv_op = components.load_component_from_url(
    'https://raw.githubusercontent.com/kubeflow/pipelines/31939086d66d633732f75300ce69eb60e9fb0269/components/XGBoost/Predict/component.yaml'
)
xgboost_train_on_parquet_op = components.load_component_from_url(
    'https://raw.githubusercontent.com/kubeflow/pipelines/0ae2f30ff24beeef1c64cc7c434f1f652c065192/components/XGBoost/Train/from_ApacheParquet/component.yaml'
)
xgboost_predict_on_parquet_op = components.load_component_from_url(
    'https://raw.githubusercontent.com/kubeflow/pipelines/31939086d66d633732f75300ce69eb60e9fb0269/components/XGBoost/Predict/from_ApacheParquet/component.yaml'
)


@dsl.pipeline(name='xgboost-sample-pipeline', pipeline_root='dummy_root')
def xgboost_pipeline():
    training_data_csv = chicago_taxi_dataset_op(
        where='trip_start_timestamp >= "2019-01-01" AND trip_start_timestamp < "2019-02-01"',
        select='tips,trip_seconds,trip_miles,pickup_community_area,dropoff_community_area,fare,tolls,extras,trip_total',
        limit=10000,
    ).output

    # Training and prediction on dataset in CSV format
    model_trained_on_csv = xgboost_train_on_csv_op(
        training_data=training_data_csv,
        label_column=0,
        objective='reg:squarederror',
        num_iterations=200,
    ).outputs['model']

    xgboost_predict_on_csv_op(
        data=training_data_csv,
        model=model_trained_on_csv,
        label_column=0,
    )

    # Training and prediction on dataset in Apache Parquet format
    training_data_parquet = convert_csv_to_apache_parquet_op(
        training_data_csv).output

    model_trained_on_parquet = xgboost_train_on_parquet_op(
        training_data=training_data_parquet,
        label_column_name='tips',
        objective='reg:squarederror',
        num_iterations=200,
    ).outputs['model']

    xgboost_predict_on_parquet_op(
        data=training_data_parquet,
        model=model_trained_on_parquet,
        label_column_name='tips',
    )

    # Checking cross-format predictions
    xgboost_predict_on_parquet_op(
        data=training_data_parquet,
        model=model_trained_on_csv,
        label_column_name='tips',
    )

    xgboost_predict_on_csv_op(
        data=training_data_csv,
        model=model_trained_on_parquet,
        label_column=0,
    )


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=xgboost_pipeline,
        package_path=__file__.replace('.py', '.json'))
