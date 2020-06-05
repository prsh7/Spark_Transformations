import argparse
import csv
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json


class DataTransformation:
    """A helper class which contains the logic to translate the file into a
  format BigQuery will accept."""

    def __init__(self):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "M:\gcp-sa-cred.json"
        pass


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        required=False,
                        help='Input file to read. This can be a local file or a file in a Google Storage Bucket.',
                        default='gs://gce-test-212506/loanData.csv')

    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='lake.usa_names_transformed')

    known_args, pipeline_args = parser.parse_known_args(argv)
    data_ingestion = DataTransformation()

    # Initiate the pipeline using the pipeline arguments passed in from the
    # command line.  This includes information like where Dataflow should
    # store temp files, and what the project id is.
    p = beam.Pipeline(options=PipelineOptions(pipeline_args))
    schema = "id:string, member_id:string, loan_amnt:string, funded_amnt:string, funded_amnt_inv:string, term:string, int_rate:string, installment:string, grade:string, sub_grade:string, emp_title:string, emp_length:string, home_ownership:string, annual_inc:string, is_inc_v:string, issue_d:string, loan_status:string, pymnt_plan:string, url:string, desc:string, purpose:string, title:string, zip_code:string, addr_state:string, dti:string, delinq_2yrs:string, earliest_cr_line:string, inq_last_6mths:string, mths_since_last_delinq:string, mths_since_last_record:string, open_acc:string, pub_rec:string, revol_bal:string, revol_util:string, total_acc:string, initial_list_status:string, out_prncp:string, out_prncp_inv:string, total_pymnt:string, total_pymnt_inv:string, total_rec_prncp:string, total_rec_int:string, total_rec_late_fee:string, recoveries:string, collection_recovery_fee:string, last_pymnt_d:string, last_pymnt_amnt:string, next_pymnt_d:string, last_credit_pull_d:string, collections_12_mths_ex_med:string, mths_since_last_major_derog:string, policy_code:string"

    (p
     | 'Read From Text' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
     | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(known_args.output,
                                                                 schema=schema,
                                                                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
     )
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
