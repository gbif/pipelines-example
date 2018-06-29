package org.gbif.example.interpretation;

import org.gbif.example.io.avro.ExampleRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.transform.Kv2Value;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common example how to use {@link ExampleTransform}/{@link
 * org.gbif.pipelines.transform.RecordTransform} in Apache Beam
 */
public class ExampleBeamPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(ExampleBeamPipeline.class);

  public static void main(String[] args) {

    LOG.info("Initializing pipeline options");
    ExampleOptions options = PipelineOptionsFactory.fromArgs(args).as(ExampleOptions.class);

    LOG.info("Creating a beam pipeline");
    Pipeline p = Pipeline.create(options);
    String inputFile = options.getInputFile();
    String targetDataDirectory = options.getDefaultTargetDirectory() + "/example-record";
    String targetIssueDirectory = options.getDefaultTargetDirectory() + "/examaple-issue";

    LOG.info("Creating transform object");
    ExampleTransform transform = ExampleTransform.create().withAvroCoders(p);

    LOG.info("STEP 1: Read verbatim avro files");
    PCollection<ExtendedRecord> verbatimRecords =
        p.apply("Read an avro file", AvroIO.read(ExtendedRecord.class).from(inputFile));

    LOG.info("STEP 2: Apply our transform");
    PCollectionTuple exampleRecordTuple = verbatimRecords.apply(transform);

    LOG.info("Getting data from transformation");
    PCollection<ExampleRecord> exampleRecords =
        exampleRecordTuple.get(transform.getDataTag()).apply(Kv2Value.create());
    LOG.info("Getting issues from transformation");
    PCollection<OccurrenceIssue> issueRecords =
        exampleRecordTuple.get(transform.getIssueTag()).apply(Kv2Value.create());

    LOG.info("STEP 3: Save to an avro file");
    exampleRecords.apply(
        "Write data to an avro file",
        AvroIO.write(ExampleRecord.class).to(targetDataDirectory).withSuffix(".avro"));
    issueRecords.apply(
        "Write issues to an avro file",
        AvroIO.write(OccurrenceIssue.class).to(targetIssueDirectory).withSuffix(".avro"));

    // Run
    LOG.info("Starting the pipeline");
    PipelineResult result = p.run();
    result.waitUntilFinish();
    LOG.info("Pipeline finished with state: {} ", result.getState());
  }
}
