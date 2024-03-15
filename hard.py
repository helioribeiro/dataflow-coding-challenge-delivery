from config import beam_options
import apache_beam as beam
from apache_beam.io import Write
from geopy.distance import geodesic

class WriteToCustomText(beam.PTransform):
    def __init__(self, file_path):
        self.file_path = file_path

    def expand(self, pcoll):
        return pcoll | beam.io.WriteToText(self.file_path, num_shards=1, file_name_suffix='.txt', shard_name_template='')

def run():
    with beam.Pipeline(options=beam_options) as pipeline:
        # Step 1: Query the data using SQL SELECT
        query = """
    SELECT
        hire.start_station_id AS start_station_id,
        hire.end_station_id AS end_station_id,
        stations.latitude AS start_station_latitude,
        stations.longitude AS start_station_longitude,
        stations_end.latitude AS end_station_latitude,
        stations_end.longitude AS end_station_longitude
    FROM
        `bigquery-public-data.london_bicycles.cycle_hire` AS hire
    JOIN
        `bigquery-public-data.london_bicycles.cycle_stations` AS stations
    ON
        hire.start_station_id = stations.id
    JOIN
        `bigquery-public-data.london_bicycles.cycle_stations` AS stations_end
    ON
        hire.end_station_id = stations_end.id
"""
        extracted_data = (
            pipeline
            | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
        )

        # Step 2: Calculate distance between stations
        def calculate_distance(row):
            start_coords = (row['start_station_latitude'], row['start_station_longitude'])
            end_coords = (row['end_station_latitude'], row['end_station_longitude'])
            distance = geodesic(start_coords, end_coords).kilometers
            return {
                'start_station_id': row['start_station_id'],
                'end_station_id': row['end_station_id'],
                'singular_distance_between_stations': distance
            }

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
        def calculate_total_distance(row, distance_data):
            start_station_id, end_station_id = row[0]
            amount_of_rides = row[1]
            distance = distance_data.get((start_station_id, end_station_id), 0)
            total_distance = amount_of_rides * distance
            return {
                'start_station_id': start_station_id,
                'end_station_id': end_station_id,
                'amount_of_rides': amount_of_rides,
                'total_distance_traveled': total_distance
            }

        total_distance_traveled = (
            counted_rides
            | 'Join with singular distances' >> beam.Map(
                lambda rides, distances: calculate_total_distance(rides, distances), 
                beam.pvalue.AsDict(calculated_distance))
        )

        # Step 5: Format the output and sort
        def format_output(row):
            return f"{row['start_station_id']},{row['end_station_id']},{row['amount_of_rides']},{row['total_distance_traveled']}"

        formatted_output = (
            total_distance_traveled
            | 'Format output' >> beam.Map(format_output)
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

if __name__ == '__main__':
    run()