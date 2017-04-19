package com.example;
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.PipelineResult.State;
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
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableOptions;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.HashMap;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.util.Preconditions;
import com.google.api.services.storage.Storage;
public class mihin
{
	private static final byte[] FAMILY = Bytes.toBytes("cf1");
        private static final byte[] bday = Bytes.toBytes("bday");
        private static final byte[] gender = Bytes.toBytes("gender");
	private static long row_id = 1;
	static String getFile(String BUCKET_NAME , String FILENAME) {
			HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        		JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
        		GoogleCredential credential = GoogleCredential.getApplicationDefault();
        		Storage storage = new Storage.Builder(httpTransport, jsonFactory, credential)
        		.setApplicationName("Google-ObjectsListExample/1.0").build();
         		Storage.Objects.Get obj = storage.objects().get(BUCKET_NAME, FILENAME);
         		HttpResponse response = obj.execute();
			String file=response.parseAsString();
			JSONParser parser = new JSONParser();
                        Object obj1 = parser.parse(file);
	       	        JSONObject jsonObject = (JSONObject) obj1;
	    		return (jsonObject.toString());
	}
	static final DoFn<String, String> MUTATION_TRANSFORM = new DoFn<String, String>() {
 		@Override
    		public void processElement(DoFn<String, String>.ProcessContext c) throws IOException{
      			String line = c.element();
			 JSONParser parser = new JSONParser();
			 try {
				Object obj = parser.parse(line);
				JSONObject jsonObject = (JSONObject) obj;
			 	JSONArray resource = (JSONArray) jsonObject.get("resources");
				 Put put_object = null ;
				for (int i = 0; i < 100; i++) {
				        put_object = new Put(Bytes.toBytes(row_id));
				        row_id = row_id +1;
            				JSONObject jsonObject1 = (JSONObject) parser.parse(resource.get(i).toString());
      					HashMap map = (HashMap) jsonObject1.get("resource");
					put_object.addColumn(FAMILY, bday, Bytes.toBytes(map.get("birthDate").toString()));
					put_object.addColumn(FAMILY, gender, Bytes.toBytes(map.get("gender").toString()));
					c.output(put_object.toString());

			 }
			 }
			catch (Exception e) {
            e.printStackTrace();
        }
    		}
	};
	public static class ProcessFile extends PTransform<PCollection<String>, PCollection<String>> {

    	@Override
    	public PCollection<String> apply(PCollection<String> inputFile) {
     	 PCollection<String> formatedFile = inputFile.apply(
         ParDo.of(FORMAT_JSON));
         return formatedFile;
    }
  }

	public static void main(String[] args) 
	{
	
		// Start by defining the options for the pipeline.
		CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder().withProjectId("healthcare-12").withInstanceId("hc-dataset").withTableId("mihin-test1").build();
		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		String BUCKET_NAME = "mihin-data";
         	String FILENAME = "Patient_entry.txt";
		// The 'gs' URI means that this is a Google Cloud Storage path
		options.setStagingLocation("gs://mihin-data/staging1");

		// Then create the pipeline.
		Pipeline p = Pipeline.create(options);
 		CloudBigtableIO.initializeForWrite(p);
		String JSON = mihin.getFile(BUCKET_NAME , FILENAME);
		PCollection<String> FormatedFile = p.apply(Create.of(JSON)).setCoder(StringUtf8Coder.of()) ;
		//p.apply(TextIO.Read.from("gs://mihin-data/Patient_entry.txt")).apply(new ProcessFile())
			//.apply(CloudBigtableIO.writeToTable(config));
			FormatedFile.apply(TextIO.Write.to("gs://mihin-data/formatedPatientGen.json"));
 				

		
			//p.apply(TextIO.Read.from("gs://mihin-data/formatedPatientGen.json"))
			p.run();
		}	
     		//.apply(TextIO.Write.to("gs://mihin-data/temp.txt"));

		

		//PCollection<String> lines=p.apply(TextIO.Read.from("gs://synpuf-data/DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"))
		//PCollection<String> fields = lines.apply(ParDo.of(new ExtractFieldsFn()));
		//p.apply(TextIO.Write.to("gs://synpuf-data/temp.txt"));
	
	
}
