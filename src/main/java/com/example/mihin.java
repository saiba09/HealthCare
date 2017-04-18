package com.example;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.opencsv.CSVParser;
import java.io.IOException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class mihin
{
	static class ExtractFieldsFn extends DoFn<String, String>  {
		
		@Override
    		public void processElement(ProcessContext c) throws IOException{
      			String line = c.element();
			  JSONParser parser = new JSONParser();
			 try {
				Object obj = parser.parse(line);
				 JSONObject jsonObject = (JSONObject) obj;
			 	JSONArray resource = (JSONArray) jsonObject.get("resources");
      			// Output each word encountered into the output PCollection.
      				c.output(resource +"ended");
			 }
			catch (Exception e) {
            e.printStackTrace();
        }
    		}
		
	}

	public static void main(String[] args) 
	{
	
		// Start by defining the options for the pipeline.
		
		DataflowPipelineOptions options = PipelineOptionsFactory.create()
    		.as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		
		// The 'gs' URI means that this is a Google Cloud Storage path
		options.setStagingLocation("gs://mihin-data/staging1");

		// Then create the pipeline.
		Pipeline p = Pipeline.create(options);

 		p.apply(TextIO.Read.from("gs://mihin-data/temp.json"))
     		.apply(ParDo.of(new ExtractFieldsFn()))
     		.apply(TextIO.Write.to("gs://mihin-data/temp.txt"));

		p.run();

		//PCollection<String> lines=p.apply(TextIO.Read.from("gs://synpuf-data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"))
		//PCollection<String> fields = lines.apply(ParDo.of(new ExtractFieldsFn()));
		//p.apply(TextIO.Write.to("gs://synpuf-data/temp.txt"));
	}

}
