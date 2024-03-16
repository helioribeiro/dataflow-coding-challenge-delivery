from config import beam_options
import apache_beam as beam
from apache_beam.io import Writer

class WriteToCustomText(beam.PTransform):
    def __init__(self, file_path):
        self.file_path = file_path

    def expand(self, pcoll):
        return pcoll | beam.FlatMap(lambda x: x) | beam.io.WriteToText(self.file_path, num_shards=1, file_name_suffix='.txt', shard_name_template='')

def calculate_distance(row):
    from geopy import distance
    start_coords = (row['start_station_latitude'], row['start_station_longitude'])
    end_coords = (row['end_station_latitude'], row['end_station_longitude'])
    distancekm = distance.geodesic(start_coords, end_coords).kilometers
    return {
        'start_station_id': row['start_station_id'],
        'end_station_id': row['end_station_id'],
        'singular_distance_between_stations': distancekm
    }

with beam.Pipeline(options=beam_options) as pipeline:
    # Step 1: Query the data using SQL SELECT
    query = """
    SELECT
        ch.start_station_id,
        ch.end_station_id,
        start_station.latitude AS start_station_latitude,
        start_station.longitude AS start_station_longitude,
        end_station.latitude AS end_station_latitude,
        end_station.longitude AS end_station_longitude
    FROM
        `bigquery-public-data.london_bicycles.cycle_hire` AS ch
    JOIN
        `bigquery-public-data.london_bicycles.cycle_stations` AS start_station
    ON
        ch.start_station_id = start_station.id
    JOIN
        `bigquery-public-data.london_bicycles.cycle_stations` AS end_station
    ON
        ch.end_station_id = end_station.id;
    """
    extracted_data = (
        pipeline
        | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
    )

    # Step 2: Calculate distance between stations    
    calculated_distance = (
        extracted_data
        | 'Calculate distance' >> beam.Map(calculate_distance)
        | 'Pair with keys' >> beam.Map(lambda row: ((row['start_station_id'], row['end_station_id']), row['singular_distance_between_stations']))
    )

    # Step 3: Count all possible combinations of 'start_station_id' and 'end_station_id'
    counted_rides = (
        calculated_distance
        | 'Map rides' >> beam.Map(lambda row: ((row[0][0], row[0][1]), 1))
        | 'Count rides' >> beam.CombinePerKey(sum)
        | 'Filter null values' >> beam.Filter(lambda kv: kv[0][0] is not None and kv[0][1] is not None)
    )

    # Step 4: Multiply 'amount_of_rides' by 'singular_distance_between_stations'
    total_distance_traveled = (
        counted_rides
        | 'Join with singular distances' >> beam.Map(
            lambda row, distance_data: {
                'start_station_id': row[0][0],
                'end_station_id': row[0][1],
                'amount_of_rides': row[1],
                'total_distance_traveled': row[1] * distance_data.get(row[0], 0)
            }, 
            beam.pvalue.AsDict(calculated_distance))
    )

    # Step 5: Format the output and sort
    formatted_output = (
    total_distance_traveled
    | 'Format output' >> beam.Map(lambda row: f"{row['start_station_id']},{row['end_station_id']},{row['amount_of_rides']},{row['total_distance_traveled']}")
    )

    # Step 6: Sort the formatted output by total_distance_traveled and limit to top 100 rows
    sorted_data = (
        formatted_output
        | 'Sort output' >> beam.transforms.combiners.Top.Largest(
            100, key=lambda x: float(x.split(',')[3])
        )
    )

    # Step 7: Save Output as .txt
    final_output = (
        sorted_data
        | 'Save output' >> WriteToCustomText(
                'gs://challenge-helio-ribeiro-delivery-hard/output/Hard_Test'
            )
        )