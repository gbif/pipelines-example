package org.gbif.example.interpretation;

import org.gbif.example.io.avro.ExampleRecord;
import org.gbif.pipelines.core.interpretation.Interpretation;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.function.BiConsumer;

/** Java function for a business logic */
public interface ExampleInterpreter
    extends BiConsumer<ExtendedRecord, Interpretation<ExampleRecord>> {

  static ExampleInterpreter interpretStepOne() {
    return (extendedRecord, interpretation) -> {
      // TODO: the place for real business
      ExampleRecord exampleRecord = interpretation.getValue();
      exampleRecord.setOne(extendedRecord.getId());
    };
  }

  static ExampleInterpreter interpretStepTwo() {
    return (extendedRecord, interpretation) -> {
      // TODO: the place for real business
      ExampleRecord exampleRecord = interpretation.getValue();
      exampleRecord.setTwo(extendedRecord.getId());
    };
  }

  static ExampleInterpreter interpretStepThree() {
    return (extendedRecord, interpretation) -> {
      // TODO: the place for real business
      ExampleRecord exampleRecord = interpretation.getValue();
      exampleRecord.setThree(extendedRecord.getId());
    };
  }
}
