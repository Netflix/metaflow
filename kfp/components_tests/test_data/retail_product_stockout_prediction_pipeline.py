from ...components import load_component_from_url

automl_create_dataset_for_tables_op = load_component_from_url(
    'https://raw.githubusercontent.com/kubeflow/pipelines/b3179d86b239a08bf4884b50dbf3a9151da96d66/components/gcp/automl/create_dataset_for_tables/component.yaml'
)
automl_import_data_from_bigquery_source_op = load_component_from_url(
    'https://raw.githubusercontent.com/kubeflow/pipelines/b3179d86b239a08bf4884b50dbf3a9151da96d66/components/gcp/automl/import_data_from_bigquery/component.yaml'
)
automl_create_model_for_tables_op = load_component_from_url(
    'https://raw.githubusercontent.com/kubeflow/pipelines/b3179d86b239a08bf4884b50dbf3a9151da96d66/components/gcp/automl/create_model_for_tables/component.yaml'
)
automl_prediction_service_batch_predict_op = load_component_from_url(
    'https://raw.githubusercontent.com/kubeflow/pipelines/b3179d86b239a08bf4884b50dbf3a9151da96d66/components/gcp/automl/prediction_service_batch_predict/component.yaml'
)
automl_split_dataset_table_column_names_op = load_component_from_url(
    'https://raw.githubusercontent.com/kubeflow/pipelines/b3179d86b239a08bf4884b50dbf3a9151da96d66/components/gcp/automl/split_dataset_table_column_names/component.yaml'
)

from typing import NamedTuple


# flake8: noqa
def retail_product_stockout_prediction_pipeline(
    gcp_project_id: str,
    gcp_region: str,
    batch_predict_gcs_output_uri_prefix: str,
    dataset_bq_input_uri:
    str = 'bq://product-stockout.product_stockout.stockout',
    dataset_display_name: str = 'stockout_data',
    target_column_name: str = 'Stockout',
    model_display_name: str = 'stockout_model',
    batch_predict_bq_input_uri:
    str = 'bq://product-stockout.product_stockout.batch_prediction_inputs',
    train_budget_milli_node_hours: 'Integer' = 1000,
) -> NamedTuple('Outputs', [('model_path', str)]):
    # Create dataset
    create_dataset_task = automl_create_dataset_for_tables_op(
        gcp_project_id=gcp_project_id,
        gcp_region=gcp_region,
        display_name=dataset_display_name,
    )

    # Import data
    import_data_task = automl_import_data_from_bigquery_source_op(
        dataset_path=create_dataset_task.outputs['dataset_path'],
        input_uri=dataset_bq_input_uri,
    )

    # Prepare column schemas
    split_column_specs = automl_split_dataset_table_column_names_op(
        dataset_path=import_data_task.outputs['dataset_path'],
        table_index=0,
        target_column_name=target_column_name,
    )

    # Train a model
    create_model_task = automl_create_model_for_tables_op(
        gcp_project_id=gcp_project_id,
        gcp_region=gcp_region,
        display_name=model_display_name,
        #dataset_id=create_dataset_task.outputs['dataset_id'],
        dataset_id=import_data_task.outputs['dataset_path'],
        target_column_path=split_column_specs.outputs['target_column_path'],
        #input_feature_column_paths=None, # All non-target columns will be used if None is passed
        input_feature_column_paths=split_column_specs.outputs['feature_column_paths'],
        optimization_objective='MAXIMIZE_AU_PRC',
        train_budget_milli_node_hours=train_budget_milli_node_hours,
    )  #.after(import_data_task)

    # Batch prediction
    batch_predict_task = automl_prediction_service_batch_predict_op(
        model_path=create_model_task.outputs['model_path'],
        bq_input_uri=batch_predict_bq_input_uri,
        gcs_output_uri_prefix=batch_predict_gcs_output_uri_prefix,
    )

    return [create_model_task.outputs['model_path']]
