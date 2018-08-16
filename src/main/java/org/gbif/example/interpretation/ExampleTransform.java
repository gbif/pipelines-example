package org.gbif.example.interpretation;

import org.gbif.example.io.avro.ExampleRecord;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.interpretation.InterpreterHandler;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.transform.RecordTransform;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import static org.gbif.example.interpretation.ExampleInterpreter.interpretStepOne;
import static org.gbif.example.interpretation.ExampleInterpreter.interpretStepThree;
import static org.gbif.example.interpretation.ExampleInterpreter.interpretStepTwo;

/**
 * Common abstraction for Apache Beam, extends RecordTransform, which extends Beam's PTransform
 * class. RecordTransform it as a typical transformation with one input format and two outputs.
 *
 * <p>Input: {@link ExtendedRecord} as a data source
 *
 * <p>Outputs: first - the main data output, in our case {@link ExampleRecord} and second common
 * type - {@link org.gbif.pipelines.io.avro.issue.OccurrenceIssue}
 *
 * <p>Example of using {@link ExampleTransform}:
 *
 * <pre>{@code
 * ExampleTransform transform = ExampleTransform.create().withAvroCoders(pipeline);
 * PCollectionTuple recordTuple = collections.apply(transform);
 *
 * }</pre>
 *
 * <p>You can get data from RecordTransform by tags, for main data use method - {@link
 * RecordTransform#getDataTag()}, for issue data use - {@link RecordTransform#getIssueTag()}
 *
 * <p>Example:
 *
 * <pre>{@code
 * PCollection<KV<String, ExampleRecord>> example = recordTuple.get(transform.getDataTag());
 *
 * or
 *
 * PCollection<ExampleRecord> example = recordTuple.get(transform.getDataTag()).apply(Kv2Value.create());
 *
 * }</pre>
 */
public class ExampleTransform extends RecordTransform<ExtendedRecord, ExampleRecord> {

  private ExampleTransform() {
    super("Interpret new record");
  }

  public static ExampleTransform create() {
    return new ExampleTransform();
  }

  @Override
  public DoFn<ExtendedRecord, KV<String, ExampleRecord>> interpret() {
    return new DoFn<ExtendedRecord, KV<String, ExampleRecord>>() {
      @ProcessElement
      public void processElement(ProcessContext context) {

        // Context element to be interpreted
        ExtendedRecord extendedRecord = context.element();
        String id = extendedRecord.getId();

        // Transformation main output
        // Interpret metadata terms and add validations
        InterpreterHandler.of(extendedRecord, new ExampleRecord())
            .withId(id)
            .using(interpretStepOne())
            .using(interpretStepTwo())
            .using(interpretStepThree())
            .consumeData(d -> context.output(getDataTag(), KV.of(id, d)))
            .consumeIssue(i -> context.output(getIssueTag(), KV.of(id, i)));
      }
    };
  }

  /**
   * If we want to use Avro as the main file type, we must register the necessary avro classes in
   * the pipeline. If you use several {@link RecordTransform}, this is the easiest way how not
   * forget to register all avro classes
   */
  @Override
  public ExampleTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(
        pipeline, OccurrenceIssue.class, ExampleRecord.class, ExtendedRecord.class);
    return this;
  }
}
