package org.gbif.example.interpretation;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

import static org.apache.beam.sdk.annotations.Experimental.Kind;

@Experimental(Kind.FILESYSTEM)
public interface ExampleOptions extends PipelineOptions {

  @Description("Default directory where the target file will be written")
  String getDefaultTargetDirectory();

  void setDefaultTargetDirectory(String targetDirectory);

  @Description(
      "Path of the input file, the path can be absolute or relative to the directory where the pipeline is running.")
  String getInputFile();

  void setInputFile(String inputFile);
}
