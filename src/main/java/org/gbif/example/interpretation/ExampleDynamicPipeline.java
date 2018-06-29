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

/**
 * If you want to add your new step after GBIF's main steps, you can follow this example:
 *
 * Example of {@link DataProcessingPipelineOptions}, you can pass arguments via main call:
 *
 * <pre>{@code
 * java -cp pipelines-example-1.0-SNAPSHOT-shaded.jar org.gbif.example.interpretation.ExampleDynamicPipeline --wsProperties=... and etc.
 *
 * Whole list of properties, check {@link DataProcessingPipelineOptions}:
 *
 *  --wsProperties=/Users/Projects/GBIF/pipelines-example/src/main/resources/ws.properties
 *  --targetParallelism=16
 *  --datasetId=0021cd20-de15-4eba-837f-335c00c154dd
 *  --attempt=1
 *  --interpretationTypes=ALL
 *  --runner=DirectRunner
 *  --defaultTargetDirectory=/Users/Projects/GBIF/inter-finished/
 *  --inputFile=/Users/Projects/GBIF/tmp/dwca-finished/0021cd20-de15-4eba-837f-335c00c154dd/1/verbatim.avro
 *  --avroCompressionType=DEFLATE
 *  --hdfsTempLocation=/Users/cgp440/Projects/GBIF/tmp/temp/
 *
 *
 * }</pre>
 *
 * <p>interpretationTypes - list of steps, GbifInterpretationPipeline contains 5 steps - COMMON,
 * LOCATION, MULTIMEDIA, TAXONOMY and TEMPORAL
 *
 * <p>
 *
 * <pre>{@code
 * For all steps use - ALL:
 * --interpretationTypes=ALL
 *
 * Or you can specify only necessary steps:
 * --interpretationTypes=TEMPORAL,EXAMPLE,LOCATION
 *
 * }</pre>
 */
public class ExampleDynamicPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(ExampleDynamicPipeline.class);

  public static void main(String[] args) {
    DataProcessingPipelineOptions options = DataPipelineOptionsFactory.create(args);
    GbifInterpretationPipeline gbifPipeline = GbifInterpretationPipeline.create(options);

    final String stepName = "EXAMPLE";

    PipelineTargetPaths paths = FsUtils.createPaths(options, stepName);
    InterpretationStepSupplier exampleStep =
        () ->
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
