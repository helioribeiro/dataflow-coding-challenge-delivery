from config import beam_options
import apache_beam as beam
from apache_beam.io import Write

class WriteToCustomText(beam.PTransform):
    def __init__(self, file_path):
        self.file_path = file_path

    def expand(self, pcoll):
        return pcoll | beam.FlatMap(lambda x: x) | beam.io.WriteToText(self.file_path, num_shards=1, file_name_suffix='.txt', shard_name_template='')


with beam.Pipeline(options=beam_options) as pipeline:
    # Step 1: BigQuery Extraction
    query = 'SELECT start_station_id, end_station_id FROM `bigquery-public-data.london_bicycles.cycle_hire`'
    extracted_data = (
        pipeline
        | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
    )

    # Step 2: Count rides for each combination of start_station_id and end_station_id
    counted_rides = (
        extracted_data
        | 'Map rides' >> beam.Map(lambda row: ((row['start_station_id'], row['end_station_id']), 1))
        | 'Count rides' >> beam.CombinePerKey(sum)
        | 'Filter null values' >> beam.Filter(lambda kv: kv[0][0] is not None and kv[0][1] is not None)
    )

    # Step 3: Format the output
    formatted_output = (
        counted_rides
        | 'Format output' >> beam.Map(lambda kv: ','.join(list(map(str, kv[0])) + [str(kv[1])]))
    )

    # Step 4: Sort the output by the amount of rides
    sorted_data = (
        formatted_output
        | 'Sort output' >> beam.Map(lambda line: (int(line.split(',')[2]), line))
        | 'Group by amount of rides' >> beam.GroupByKey()
        | 'Flatten grouped data' >> beam.FlatMap(lambda kv: kv[1])
    )

    # Step 5: Limit to 100 rows
    limited_data = (
        sorted_data
        | 'Limit to 100 rows' >> beam.transforms.combiners.Top.Largest(100, key=lambda x: int(x.split(',')[2]))
    )

    # Step 6: Save Output as .txt
    final_output = (
        limited_data
        | 'Save output' >> WriteToCustomText(
            'gs://challenge-helio-ribeiro-delivery-easy/output/Easy_Test'
        )
    )
