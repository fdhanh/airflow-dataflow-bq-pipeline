import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json

class transformJSON(beam.DoFn):
    """
    Transformation:
    - Normalize data 
    - Get data album id, artist, song title, release date
    """

    def process(self, element):
        file_name, content = json.loads(element)
        code = content["code"]
        for item in content['items']:
            yield {
                "code": code,
                "video_id": item["id"],
                "published_at": item["snippet"]["publishedAt"],
                "channel_id": item["snippet"]["channel_id"],
                "video_title": item["snippet"]["title"],
                "channel_title": item["snippet"]["channelTitle"],
                "license": item["status"]["license"],
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