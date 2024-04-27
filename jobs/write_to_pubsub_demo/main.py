# このサンプルコードを書き換えたもの
# https://cloud.google.com/dataflow/docs/guides/write-to-pubsub?hl=ja
import argparse
from typing import Any, Dict, List
from dotenv import load_dotenv
import os
import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.io import PubsubMessage
from apache_beam.io import WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions

from typing_extensions import Self

load_dotenv()
GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']
GCS_BUCKET_NAME = os.environ['GCS_BUCKET_NAME']
DEMO_TOPIC_ID = os.environ['DEMO_TOPIC_ID']
TOPIC = 'projects/{}/topics/{}'.format(GCP_PROJECT_ID, DEMO_TOPIC_ID)
JOB_NAME = 'write-to-pubsub-demo'

def item_to_message(item: Dict[str, Any]) -> PubsubMessage:
    # Re-import needed types. When using the Dataflow runner, this
    # function executes on a worker, where the global namespace is not
    # available. For more information, see:
    # https://cloud.google.com/dataflow/docs/guides/common-errors#name-error
    from apache_beam.io import PubsubMessage

    attributes = {
        'buyer': item['name'],
        'timestamp': str(item['ts'])
    }
    data = bytes(item['product'], 'utf-8')

    return PubsubMessage(data=data, attributes=attributes)

def write_to_pubsub():

    # Parse the pipeline options passed into the application. Example:
    #     --topic=$TOPIC_PATH --streaming
    # For more information, see
    # https://beam.apache.org/documentation/programming-guide/#configuring-pipeline-options

    example_data = [
        {'name': 'Robert', 'product': 'TV', 'ts': 1613141590000},
        {'name': 'Maria', 'product': 'Phone', 'ts': 1612718280000},
        {'name': 'Juan', 'product': 'Laptop', 'ts': 1611618000000},
        {'name': 'Rebeca', 'product': 'Video game', 'ts': 1610000000000}
    ]
    options = PipelineOptions()

    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = GCP_PROJECT_ID
    google_cloud_options.job_name = JOB_NAME
    google_cloud_options.staging_location = '{}/binaries'.format(GCS_BUCKET_NAME)
    google_cloud_options.temp_location = '{}/temp'.format(GCS_BUCKET_NAME)
    google_cloud_options.region = 'asia-northeast1'

    options.view_as(WorkerOptions).autoscaling_algorithm = 'THROUGHPUT_BASED'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    p = beam.Pipeline(options=options)
    (p
      | "Create elements" >> beam.Create(example_data)
      | "Convert to Pub/Sub messages" >> beam.Map(item_to_message)
      | WriteToPubSub(
            topic=TOPIC,
            with_attributes=True))
    p.run()
    print('Pipeline ran successfully.')

if __name__ == "__main__":
    write_to_pubsub()