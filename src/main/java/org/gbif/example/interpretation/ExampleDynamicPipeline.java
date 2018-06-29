package org.gbif.example.interpretation;

import org.gbif.example.io.avro.ExampleRecord;
import org.gbif.pipelines.assembling.GbifInterpretationPipeline;
import org.gbif.pipelines.assembling.interpretation.steps.InterpretationStep;
import org.gbif.pipelines.assembling.interpretation.steps.InterpretationStepSupplier;
import org.gbif.pipelines.assembling.interpretation.steps.PipelineTargetPaths;
import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;
import org.gbif.pipelines.utils.FsUtils;

import org.apache.beam.sdk.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleDynamicPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(ExampleDynamicPipeline.class);

  public static void main(String[] args) {
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(args);
    GbifInterpretationPipeline gbifPipeline = GbifInterpretationPipeline.create(options);

    final String stepName = "EXAMPLE";

    PipelineTargetPaths paths = FsUtils.createPaths(options, stepName);
    InterpretationStepSupplier exampleStep = () ->
      InterpretationStep.<ExampleRecord>newBuilder()
        .interpretationType(stepName)
        .avroClass(ExampleRecord.class)
        .transform(ExampleTransform.create())
        .dataTargetPath(paths.getDataTargetPath())
        .issuesTargetPath(paths.getIssuesTargetPath())
        .tempDirectory(paths.getTempDir())
        .avroCodec(gbifPipeline.getAvroCodec())
        .build();

    gbifPipeline.addNewStep(stepName, exampleStep);

    LOG.info("Run the pipeline");
    Pipeline pipeline = gbifPipeline.get();
    pipeline.run().waitUntilFinish();
    LOG.info("The pipeline has been finished!");
  }

}
