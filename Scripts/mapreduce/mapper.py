import sys
import avro.datafile
import avro.io
import io
import datetime

# Reference date (May 27, 2025)
REFERENCE_DATE = datetime.datetime(2025, 5, 27).timestamp() * 1000  # Convert to milliseconds

def read_avro_from_stdin():
    try:
        # Read Avro data from stdin
        input_data = sys.stdin.buffer.read()
        input_stream = io.BytesIO(input_data)
        reader = avro.datafile.DataFileReader(input_stream, avro.io.DatumReader())
        for record in reader:
            dob = record.get('dob')
            if dob is not None:
                # Calculate age in years
                age = (REFERENCE_DATE - dob) / (1000 * 60 * 60 * 24 * 365.25)
                print(f"1\t{age:.2f}")
        reader.close()
    except Exception as e:
        print(f"Error in mapper: {e}", file=sys.stderr)

if __name__ == "__main__":
    read_avro_from_stdin()