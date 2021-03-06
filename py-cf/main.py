from google.cloud import bigquery
from datetime import date, datetime
import base64
import os

def run_cf(event, context):
    if (context == "local"):
        if 'QUERY_DATE' in os.environ:
            qdate = os.environ.get('QUERY_DATE')
        else:
            today = date.today()
            qdate = today.strftime('%Y-%m-%d')
    else:
        # this is triggered from pubsub
        print("Executing from an Event in Pub/Sub")

        # debug
        print("context is {}".format(context))
        ts = context.timestamp

        # format of timestamp 2020-07-22T22:05:01.125Z
        d = datetime.strptime(ts.split('T')[0], "%Y-%m-%d")
        qdate = d.strftime("%Y-%m-%d")

        if 'QUERY_DATE' in os.environ:
            qdate = os.environ.get('QUERY_DATE')

        # debug
        print("qdate is {}".format(qdate))

    #     if 'data' in event:
    #         name = base64.b64decode(event['data']).decode('utf-8')

    # prepare BQ info
    bq_dataset = os.environ.get('BQ_DATASET')
    bq_table = os.environ.get('BQ_TABLE')
    holiday = check_holiday(qdate,bq_dataset,bq_table)
    
    # debugging
    # print(holiday)

    if holiday:
        print("{} is a holiday".format(qdate))
    else:
        print("{} is not a holiday".format(qdate))
    
def check_holiday(qdate, bq_dataset, bq_table):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    query = "SELECT Date FROM {}.{} WHERE Date = @current_date".format(bq_dataset, bq_table)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("current_date", "DATE", qdate),
        ]
    )
    # Run the Query in Big Query
    query_job = client.query(query, job_config=job_config, project=os.environ.get('PROJECT_ID'))
    result = query_job.result()

    # for debugging
    # for row in result:
    #     print(row)

    return (result.total_rows != 0)

# for debugging locally
if __name__ == "__main__":
    (event, context) = ("local","local")
    start_function(event, context)