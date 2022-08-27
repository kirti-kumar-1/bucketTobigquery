from flask import jsonify
from gcs_co import upload_file_to_bucket
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

# # @app.route('/upload',methods=["POST"])
# f = open('/home/kirtikumar/quickstart/contract-sample-test.csv', 'r')
# contents = f.read()
# # contents = contents.decode('utf-8').strip()
# # print(contents)
# # if len(contents.split("\n")) > 101 :
# #     return jsonify({"message":"rows must be less then 100","error":1,"filename":time_stamp+"-"+f.filename}),400

# for item in contents.split("\n"):
#     if ","  in item :
#         if item[0] == '''"'''  :
#             pass
#         # else :
#         #     return jsonify({"message":"multiple colums found","error":2,"filename":time_stamp+"-"+f.filename}),400

# upload_file_to_bucket("test-bucke", contents, 'sample-contracts.csv')
# # print(jsonify({"message":"file uploaded","error":0,"filename":"done"}),200)

def upload_file_to_bigQuery():
    # TODO(developer): Set table_id to the ID of the table to create.
    table_id = "da-training-2022-poc.test_data.sample-contracts"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("supplier", "STRING"),
            bigquery.SchemaField("Agreement_short_discription", "STRING"),
            bigquery.SchemaField("Start_date", "STRING"),
            bigquery.SchemaField("End_date", "STRING"),
            bigquery.SchemaField("Value", "STRING"),
            bigquery.SchemaField("Value_currency_code", "STRING"),
            bigquery.SchemaField("agreement_type", "STRING"),
            bigquery.SchemaField("agreement_business_criticality", "STRING"),
            bigquery.SchemaField("status", "STRING"),
        ],
        skip_leading_rows=1,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
    )
    uri = "gs://test-bucke/sample-contracts.csv"

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

upload_file_to_bigQuery()