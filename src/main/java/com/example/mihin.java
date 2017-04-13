package com.example;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.*;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import java.io.IOException;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableOptions;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.cloud.dataflow.sdk.values.KV;
//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
//import org.json.simple.parser.JSONParser;
import org.json.*;
import org.json.simple.parser.JSONParser;
import com.google.cloud.dataflow.sdk.transforms.*;
// import org.json.simple.JSONArray;
// import org.json.simple.JSONObject;
// import org.json.simple.parser.JSONParser;
// import org.json.*

import org.apache.avro.Schema;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.io.File;
public class  mihin
{
/* Class for merging and creating json object */
public class getJsonFn extends CombineFn<String, getJsonFn.JsonGet, String> {
public static class JsonGet implements SerializableFunction<Iterable<String>, String> {
    @Override
    public String apply(Iterable<String> input) {
      String obj = "";
      for (String item : input) {
        obj = obj +" "+ item;
      }
      return obj;
    }
  }
	 }
	
  private static final byte[] FAMILY = Bytes.toBytes("cf1");
   private static final byte[] column = Bytes.toBytes("column");
//     //private static final byte[] death_date = Bytes.toBytes("death_date");
    private static long row_id = 0;
    //private static final byte[] SEX = Bytes.toBytes("sex");

 static final DoFn<String, String> MUTATION_TRANSFORM = new DoFn<String, String>() {
  	private static final long serialVersionUID = 1L;

  @Override
  public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {
        JSONParser parser = new JSONParser();

  			String line = c.element();
			 JSONObject json = (JSONObject) parser.parse(line);
			 JSONObject jsonObject = (JSONObject) json;
          //  System.out.println(jsonObject);

          		  JSONArray resource = (JSONArray) jsonObject.get("resources");
           // System.out.println(name);
          
//             	System.out.println("Record : "+ i);
            	
                JSONObject jsonObject1 = (JSONObject) parser.parse(resource.get(0).toString());	 	
	  
	  
	  
	  
 			 Put put_object = new Put(Bytes.toBytes(row_id));
 			 	row_id = row_id + 1;
        			    byte[] data = Bytes.toBytes( jsonObject1.toString() );

   	 				 put_object.addColumn(FAMILY, column,data);
 			// 		 put_object.addColumn(FAMILY, death_date, Bytes.toBytes(parts[2])));
   					 c.output(jsonObject1.toString());


  }
};
		
	

	public static void main(String[] args) 
	{
		// config object for writing to bigtable

		 CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder().withProjectId("healthcare-12").withInstanceId("hc-dataset").withTableId("mihin-test").build();

		// Start by defining the options for the pipeline.
		
		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		
		// The 'gs' URI means that this is a Google Cloud Storage path
		options.setStagingLocation("gs://mihin-data/staging1");

		// Then create the pipeline.
		
  		//Schema schema = new Schema.Parser().parse(new File("gs://mihin-data/Patient_entry_Schema.txt"));
		Pipeline p = Pipeline.create(options);
		CloudBigtableIO.initializeForWrite(p);
	//	PCollection<GenericRecord> records =

		
                                  //  .withSuffix(".avro"));


		//PCollection<String> lines= 

		PCollection<String> lines= p.apply(TextIO.Read.named("Reading MIHIN Data").from("gs://mihin-data/Patient_entry.txt"));
		PCollection<String> obj = lines.apply(
   		 Combine.globally(new getJsonFn.JsonGet()));
		
		obj.apply(ParDo.named("Mihin data flowing to BigTable").of(MUTATION_TRANSFORM))
			// .apply(CloudBigtableIO.writeToTable(config));
			 .apply(TextIO.Write.named("Writing to temp loc").to("gs://mihin-data/temp.txt"));
			//PCollection<String> fields = lines.apply(ParDo.of(new ExtractFieldsFn()));
		//p.apply(TextIO.Write.to("gs://synpuf-data/temp.txt"));
		p.run();
	}

}
