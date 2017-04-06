package com.example;

import com.google.cloud.dataflow.sdk.Pipeline;
com.google.cloud.dataflow.sdk.io.*;
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
import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
// import org.json.simple.JSONArray;
// import org.json.simple.JSONObject;
// import org.json.simple.parser.JSONParser;
// import org.json.*
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class  mihin
{
	
 private static final byte[] FAMILY = Bytes.toBytes("patient_entry");
  private static final byte[] patient_id = Bytes.toBytes("patient_id");
    //private static final byte[] death_date = Bytes.toBytes("death_date");
    private static long row_id = 0;
    //private static final byte[] SEX = Bytes.toBytes("sex");

 static final DoFn<String, String> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
  	private static final long serialVersionUID = 1L;

  @Override
  public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {

  			String line = c.element();
		 	// CSVParser csvParser = new CSVParser();
 			// String[] parts = csvParser.parseLine(line);

      			// Output each word encountered into the output PCollection.
       			
         			// c.output(part);
 			// Put put_object = new Put(Bytes.toBytes(row_id);
 			// 	row_id = row_id + 1;
    //    			    byte[] data[0] = Bytes.toBytes( parts[0] );

   	// 				 put_object.addColumn(FAMILY, beneficiry_id,data[0]));
 			// 		 put_object.addColumn(FAMILY, death_date, Bytes.toBytes(parts[2])));
   					 c.output(line);


  }
};
		
	

	public static void main(String[] args) 
	{
		// config object for writing to bigtable

		// CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder().withProjectId("healthcare-12").withInstanceId("hc-dataset").
		// withTableId("mihin_data").build();

		// Start by defining the options for the pipeline.
		
		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		
		// The 'gs' URI means that this is a Google Cloud Storage path
		options.setStagingLocation("gs://mihin_data/staging1");

		// Then create the pipeline.
		Pipeline p = Pipeline.create(options);
		CloudBigtableIO.initializeForWrite(p);
	//	PCollection<GenericRecord> records =
		 p.apply(AvroIO.Read.named("Reading MIHIN Data").from("gs://mihin-data/Patient_entry.txt").withSchema(AvroAutoGenClass.class)).apply(ParDo.named("Mihin data flowing to BigTable").of(MUTATION_TRANSFORM)).apply(TextIO.Write.named("Writing to temp loc").to("gs://mihin-data/temp.txt"));
		p.run();

		//PCollection<String> lines=p.apply(TextIO.Read.from("gs://synpuf-data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"))
		//PCollection<String> fields = lines.apply(ParDo.of(new ExtractFieldsFn()));
		//p.apply(TextIO.Write.to("gs://synpuf-data/temp.txt"));
	}

}
