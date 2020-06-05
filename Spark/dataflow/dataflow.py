a = "Dataflow is fully managed and auto configured. You just deploy your pipeline"
b = "Dataflow doesn't just execute Apache Beam transforms as is, it optimizes the graph, fusing operations efficiently" \
    "  as we see with C and D here combining into a single step. " \
    "Also, it doesn't wait for a previous step to finish before starting a new SAP, as long as there is no dependency. " \
    "We see with this with A, and the groupByKey operation happening at the bottom."
c = "Third, autoscaling happens step by step in the middle of a job. As a job needs more resources," \
    " it receives more resources automatically. " \                                                                                        
    "You don't have to manually scale resources to match job needs, and you don't pay for VM " \
    "resources that aren't being used. " \
    "Dataflow will turn down the workers, as the job demand decrease." \
    " All this happens while maintaining strong streaming semantics."

import apache_beam as beam

def my_grep(line, term):
   if line.startswith(term):
      yield line

PROJECT='qwiklabs-gcp-02-9e7cd4a5e0cf'
BUCKET='qwiklabs-gcp-02-9e7cd4a5e0cf'

def run():
   argv = [
      '--project={0}'.format(PROJECT),
      '--job_name=examplejob2',
      '--save_main_session',
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      '--region=us-central1',
      '--runner=DataflowRunner'
   ]

   p = beam.Pipeline(argv=argv)
   input = 'gs://{0}/javahelp/*.java'.format(BUCKET)
   output_prefix = 'gs://{0}/javahelp/output'.format(BUCKET)
   searchTerm = 'import'

   # find all lines that contain the searchTerm
   (p
      | 'GetJava' >> beam.io.ReadFromText(input)
      | 'Grep' >> beam.FlatMap(lambda line: my_grep(line, searchTerm))
      | 'write' >> beam.io.WriteToText(output_prefix)
   )

   p.run()

if __name__ == '__main__':
   run()

#######################################################################
# Serverless Data Analysis with Dataflow: MapReduce in Dataflow (Python)
########################################################################










