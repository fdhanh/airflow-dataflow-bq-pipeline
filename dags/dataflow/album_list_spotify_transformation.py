import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

class transformJSON(beam.DoFn):
    """
    Transformation:
    - Normalize data 
    - Get data about album
    """

    def process(self, element):
        file_name, content = json.loads(element)
        yield {
            "album_id": content['id'],
            "name": content['name'],
            "release_date": content['release_date'],
            "album_type": content['album_type'],
            "copyrights": content['copyrights'],
            "label": content['label'],
            "updated_at": file_name.split('/')[-3]
        }

def run(input_path, output_path): 
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
    )

    p = beam.Pipeline(pipeline_options)    

    (
        p
        | "Read JSON Files" >> beam.io.ReadFromTextWithFilename(input_path)
        | "Normalize JSON" >> beam.ParDo(transformJSON())
        | "Convert to JSON" >> beam.Map(lambda x: json.dumps(x))
        | "Write to Single File" >> beam.io.WriteToText(output_path, file_name_suffix='.json')
    )

    p.run()