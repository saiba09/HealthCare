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
    private static final byte[] SEX = Bytes.toBytes("sex");
    private static final byte[] ESRDI = Bytes.toBytes("esrdi");
    private static final byte[] state_code = Bytes.toBytes("state_code");
    private static final byte[] ANUAL_MEDICLAIM_INPATIENT = Bytes.toBytes("anual_mediclaim_inpatient");
    private static final byte[] ANUAL_MEDICLAIM_OUTPATIENT = Bytes.toBytes("anual_mediclaim_outpatient");
    private static final byte[] SP_ALZHDMTA = Bytes.toBytes("SP_ALZHDMTA");
    private static final byte[] SP_CHF = Bytes.toBytes("SP_CHF");
    private static final byte[] SP_CHRNKIDN = Bytes.toBytes("SP_CHRNKIDN");
    private static final byte[] SP_CNCR = Bytes.toBytes("SP_CNCR");
    private static final byte[] SP_COPD = Bytes.toBytes("SP_COPD");
    private static final byte[] SP_DEPRESSN = Bytes.toBytes("SP_DEPRESSN");
    private static final byte[] SP_DIABETES = Bytes.toBytes("SP_DIABETES");
    private static final byte[] SP_ISCHMCHT = Bytes.toBytes("SP_ISCHMCHT");
    private static final byte[] SP_OSTEOPRS = Bytes.toBytes("SP_OSTEOPRS");
    private static final byte[] SP_RA_OA = Bytes.toBytes("SP_RA_OA");
    private static final byte[] SP_STRKETIA = Bytes.toBytes("SP_STRKETIA");
    private static long row_id = 1;
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
			put_object.addColumn(FAMILY, SEX, Bytes.toBytes(parts[3]));
			put_object.addColumn(FAMILY, ESRDI, Bytes.toBytes(parts[5]));
			put_object.addColumn(FAMILY, state_code, Bytes.toBytes(parts[6]));
			put_object.addColumn(FAMILY, SP_ALZHDMTA, Bytes.toBytes(parts[12]));
			put_object.addColumn(FAMILY, SP_CHF, Bytes.toBytes(parts[13]));
			put_object.addColumn(FAMILY, SP_CHRNKIDN, Bytes.toBytes(parts[14]));
			put_object.addColumn(FAMILY, SP_CNCR, Bytes.toBytes(parts[15]));
			put_object.addColumn(FAMILY, SP_COPD, Bytes.toBytes(parts[16]));
			put_object.addColumn(FAMILY, SP_DEPRESSN, Bytes.toBytes(parts[17]));
			put_object.addColumn(FAMILY, SP_DIABETES, Bytes.toBytes(parts[18]));
			put_object.addColumn(FAMILY, SP_ISCHMCHT, Bytes.toBytes(parts[19]));
			put_object.addColumn(FAMILY, SP_OSTEOPRS, Bytes.toBytes(parts[20]));
			put_object.addColumn(FAMILY, SP_RA_OA, Bytes.toBytes(parts[21]));
			put_object.addColumn(FAMILY, SP_STRKETIA, Bytes.toBytes(parts[22]));
			put_object.addColumn(FAMILY, ANUAL_MEDICLAIM_INPATIENT, Bytes.toBytes(parts[23]));
			put_object.addColumn(FAMILY, ANUAL_MEDICLAIM_OUTPATIENT, Bytes.toBytes(parts[27]));
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
p.apply(TextIO.Read.named("Reading from File").from("gs://synpuf_data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv")).apply(ParDo.named("Processing Synpuf data").of(MUTATION_TRANSFORM)).apply(CloudBigtableIO.writeToTable(config));
	
		p.run();

		//PCollection<String> lines=p.apply(TextIO.Read.from("gs://synpuf-data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"))
		//PCollection<String> fields = lines.apply(ParDo.of(new ExtractFieldsFn()));
		//p.apply(TextIO.Write.to("gs://synpuf-data/temp.txt"));
	}

}
