# Configuration parameters for Dataflow pipeline

from apache_beam.options.pipeline_options import PipelineOptions

beam_options = PipelineOptions(
    runner='DataflowRunner',
    project='ml6-helio-ribeiro-delivery',
    staging_location='gs://challenge-helio-ribeiro-staging-temp/staging',
    temp_location='gs://challenge-helio-ribeiro-staging-temp/temp',
    region='europe-west1',
    setup_file = './setup.py'
)