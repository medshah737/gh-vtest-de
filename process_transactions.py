import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.dataframe.io import read_csv
import gzip
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import unittest
from apache_beam.transforms import combiners

gsfile='gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'

csvout=""
schema="timestamp,transaction_amount"

# Return only transactions above 20
def filter_transactions(data):
    print(data)
    global csvout
    res=data['timestamp'],data['transaction_amount']
    if data['transaction_amount'] > 20:
        csvout+=(str(res).replace("(","").replace(")","").replace("'","") + "\n")
        return data['transaction_amount']

# Set datatype for transactions
def convert_types(data):
    if 'transaction_amount' in data:
        data['transaction_amount'] = float(data['transaction_amount'])  
    else:
        None
    return data

def validate_data(data):
    kvtotals = {"testdata": data}
    with TestPipeline() as p:
      output = input | beam.combiners.Count.PerElement()
      check = assert_that(
                        kvtotals | beam.Map(lambda k_v: (k_v[0].name, k_v[1])),
                        equal_to([('timestamp', 1), ('transaction_amount', 1)])
                        )
      print(output)
      #print(check)

if __name__ == '__main__':
    p = beam.Pipeline(options=PipelineOptions())
    (p | 'readData' >> beam.io.ReadFromText(gsfile, skip_header_lines =1)
                   | 'getCols' >> beam.Map(lambda x: x.split(','))
                   | 'toDictObj' >> beam.Map(lambda x: {"timestamp": x[0], "origin": x[1], "destination": x[2], "transaction_amount": x[3]}) 
                   | 'defineDT' >> beam.Map(convert_types)
                   | 'filtered' >> beam.Filter(filter_transactions)
                   | 'pairGroup' >> beam.Map(lambda pair: (','.join(pair).encode("ascii", errors="ignore").decode(), 1))
                   | 'groupSum' >> beam.CombinePerKey(sum)
                   #| 'test' >> beam.Map(validate_data)
            )
    result = p.run() 
    print(csvout)
    with gzip.open('output.gz', 'wt') as f:
        f.write(csvout)
