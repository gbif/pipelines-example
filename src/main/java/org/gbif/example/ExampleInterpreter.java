package org.gbif.example;

import org.gbif.example.io.avro.ExampleRecord;
import org.gbif.pipelines.core.interpretation.Interpretation;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.util.function.Function;

/** Java function for a business logic*/
public interface ExampleInterpreter
    extends Function<ExtendedRecord, Interpretation<ExtendedRecord>> {

  static ExampleInterpreter interpretStepOne(ExampleRecord exampleRecord) {
    return (ExtendedRecord extendedRecord) -> {

      // Create interpretation
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);

      // TODO: the place for real business
      exampleRecord.setOne(extendedRecord.getId());

      return interpretation;
    };
  }

  static ExampleInterpreter interpretStepTwo(ExampleRecord exampleRecord) {
    return (ExtendedRecord extendedRecord) -> {

      // Create interpretation
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);

      // TODO: the place for real business
      exampleRecord.setTwo(extendedRecord.getId());

      return interpretation;
    };
  }

  static ExampleInterpreter interpretStepThree(ExampleRecord exampleRecord) {
    return (ExtendedRecord extendedRecord) -> {

      // Create interpretation
      Interpretation<ExtendedRecord> interpretation = Interpretation.of(extendedRecord);

      // TODO: the place for real business
      exampleRecord.setThree(extendedRecord.getId());

      return interpretation;
    };
  }
}
