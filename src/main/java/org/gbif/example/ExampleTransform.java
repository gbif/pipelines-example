package org.gbif.example;

import org.gbif.example.io.avro.ExampleRecord;
import org.gbif.pipelines.common.beam.Coders;
import org.gbif.pipelines.core.interpretation.Interpretation;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.issue.OccurrenceIssue;
import org.gbif.pipelines.io.avro.issue.Validation;
import org.gbif.pipelines.transform.RecordTransform;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/** Common abstraction for beam */
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
        List<Validation> validations = new ArrayList<>();

        // Transformation main output
        ExampleRecord exampleRecord = ExampleRecord.newBuilder().setId(id).build();

        Interpretation.of(extendedRecord)
            .using(ExampleInterpreter.interpretStepOne(exampleRecord))
            .using(ExampleInterpreter.interpretStepTwo(exampleRecord))
            .using(ExampleInterpreter.interpretStepThree(exampleRecord))
            .forEachValidation(trace -> validations.add(toValidation(trace.getContext())));

        // Additional output
        if (!validations.isEmpty()) {
          OccurrenceIssue issue =
              OccurrenceIssue.newBuilder().setId(id).setIssues(validations).build();
          context.output(getIssueTag(), KV.of(id, issue));
        }

        // Main output
        context.output(getDataTag(), KV.of(exampleRecord.getId(), exampleRecord));
      }
    };
  }

  @Override
  public ExampleTransform withAvroCoders(Pipeline pipeline) {
    Coders.registerAvroCoders(
        pipeline, OccurrenceIssue.class, ExampleRecord.class, ExtendedRecord.class);
    return this;
  }
}
