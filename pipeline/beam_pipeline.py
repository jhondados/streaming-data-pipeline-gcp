"""Apache Beam streaming pipeline with enrichment."""
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StreamingOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import json
from datetime import datetime

class ParseAndValidate(beam.DoFn):
    def process(self, element):
        try:
            event = json.loads(element.decode("utf-8"))
            if not all(k in event for k in ["event_id", "user_id", "timestamp", "event_type"]):
                yield beam.pvalue.TaggedOutput("invalid", element)
                return
            event["processed_at"] = datetime.utcnow().isoformat()
            event["day_partition"] = event["timestamp"][:10]
            yield event
        except Exception as e:
            yield beam.pvalue.TaggedOutput("invalid", {"raw": str(element), "error": str(e)})

def build_pipeline(project: str, subscription: str, bq_table: str):
    opts = PipelineOptions(streaming=True, project=project,
        runner="DataflowRunner", region="us-central1",
        autoscaling_algorithm="THROUGHPUT_BASED", max_num_workers=50)
    with beam.Pipeline(options=opts) as p:
        raw = p | "Read" >> ReadFromPubSub(subscription=subscription)
        parsed, invalid = (raw | "Parse" >> beam.ParDo(ParseAndValidate())
                          .with_outputs("invalid", main="valid"))
        (parsed | "WriteBQ" >> WriteToBigQuery(
            table=bq_table, create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND"))
        (invalid | "WriteErrors" >> WriteToBigQuery(table=f"{bq_table}_errors",
            create_disposition="CREATE_IF_NEEDED", write_disposition="WRITE_APPEND"))
    return p
