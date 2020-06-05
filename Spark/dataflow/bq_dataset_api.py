from google.cloud import bigquery
import pandas_gbq
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "M:\gcp-sa-cred.json"

bq_client = bigquery.Client()
datasets_obj_list = list(bq_client.list_datasets())

itr = 1
dataset_name_list = []
for dataset in datasets_obj_list:
    print("Dataset-{} -> {}".format(itr, dataset.dataset_id))
    dataset_name_list.append(dataset.dataset_id)
    itr = itr + 1

queries_each_ds = []
queries_each_ds_tables = []
if len(dataset_name_list) >= 1:
    for dataset in dataset_name_list:
        queries_each_ds.append("SELECT * FROM {}.INFORMATION_SCHEMA.TABLES".format(dataset))
        queries_each_ds_tables.append("""
        SELECT
            project_id,
            dataset_id,
            table_id,
            TIMESTAMP_MILLIS(creation_time) AS creation_time,
            TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time,
            row_count,
            TRUNC(size_bytes*POW(10,-9), 4) size_gigaBytes,
            CASE
                WHEN type=1 THEN 'Table'
                WHEN type=2 THEN 'View'
                END AS type
        FROM {}.__TABLES__""".format(dataset))


querystr = " UNION ALL ".join(queries_each_ds)
df = pandas_gbq.read_gbq(query=querystr)
df.to_csv('dataset_table_relation.csv', index=False)

querystr = " UNION ALL ".join(queries_each_ds_tables) + " ORDER BY size_gigaBytes desc"
df = pandas_gbq.read_gbq(query=querystr)
df.to_csv('dataset_table_size_ralation.csv', index=False)

source_uris = ""
bq_client.load_table_from_uri(source_uris=source_uris, destination='', job_config='')



