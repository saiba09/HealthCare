package com.example;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.opencsv.CSVParser;
import java.io.IOException;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableOptions;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
public class Synpuf
{
 private static final byte[] FAMILY = Bytes.toBytes("beneficiary-summary");
  private static final byte[] beneficiry_id = Bytes.toBytes("beneficiry_id");
    private static final byte[] death_date = Bytes.toBytes("death_date");
   private static long row_id = 0;
    //private static final byte[] SEX = Bytes.toBytes("sex");

static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
  private static final long serialVersionUID = 1L;

  @Override
  public void processElement(DoFn<String, Mutation>.ProcessContext c) throws Exception {

  	String line = c.element();
		 	CSVParser csvParser = new CSVParser();
 		String[] parts = csvParser.parseLine(line);

      			// Output each word encountered into the output PCollection.
       			
         			// c.output(part);
       			
   				Put put_object = new Put(Bytes.toBytes(row_id));
row_id = row_id +1;	
     			    byte[] data = Bytes.toBytes( parts[0]);
   					put_object.addColumn(FAMILY, beneficiry_id,data);
   					put_object.addColumn(FAMILY, death_date, Bytes.toBytes(parts[2]));
					c.output(put_object);
  }
};
		
	

	public static void main(String[] args) 
	{
		// config object for writing to bigtable

		CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder().withProjectId("healthcare-12").withInstanceId("hc-dataset").withTableId("synpuf-data").build();

		// Start by defining the options for the pipeline.
		
		DataflowPipelineOptions  options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		
		// The 'gs' URI means that this is a Google Cloud Storage path
		options.setStagingLocation("gs://synpuf_data/staging1");

		// Then create the pipeline.
		Pipeline p = Pipeline.create(options);
			CloudBigtableIO.initializeForWrite(p);
 		p.apply(TextIO.Read.from("gs://synpuf_data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv")).apply(ParDo.named("Loading to Bigtable").of(MUTATION_TRANSFORM)).apply(CloudBigtableIO.writeToTable(config));
	
		p.run();

		//PCollection<String> lines=p.apply(TextIO.Read.from("gs://synpuf-data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"))
		//PCollection<String> fields = lines.apply(ParDo.of(new ExtractFieldsFn()));
		//p.apply(TextIO.Write.to("gs://synpuf-data/temp.txt"));
	}

}
