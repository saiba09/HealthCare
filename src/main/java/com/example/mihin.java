package com.example;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PDone;
import java.io.IOException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableScanConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import java.util.HashMap;
import com.utils.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class mihin
{
	private static final String BUCKET_NAME = "mihin-data";
        private static final FormatFile fileFormater = new FormatFile();
        private static final Logger LOGGER = Logger.getLogger(mihin.class.getName());
	
	
	  
	   
	    static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
 	     private static long row_id = 1;    
	    private static final long serialVersionUID = 1L;
 	    private static final byte[] FAMILY = Bytes.toBytes("ColumnFamily1");
	    private static final byte[] BIRTHDATE = Bytes.toBytes("birthdate");
	    private static final byte[] GENDER = Bytes.toBytes("gender");
	    private static final byte[] CITY = Bytes.toBytes("city");
	    private static final byte[] NAME = Bytes.toBytes("name");
	    private static final byte[] P_ID = Bytes.toBytes("p_id");
	    private static final byte[] POSTALCODE = Bytes.toBytes("postalcode");
	    private static final byte[] STATE = Bytes.toBytes("state");

		@SuppressWarnings("unused")
		@Override
    		public void processElement(DoFn<String, Mutation>.ProcessContext c) throws IOException{
      			String line = c.element();
      			JSONParser parser = new JSONParser();
      			 try {
      				Object object = parser.parse(line);
      				JSONObject jsonObject = (JSONObject) object;
      			 	JSONArray resource = (JSONArray) jsonObject.get("resources");
      				Put put_object = null ;
      				String patientName = "";
      				String city = "";
      				String state = "";
      				String postalCode ="";
      				for (int i = 0; i < 100; i++) {
      				    put_object = new Put(Bytes.toBytes(row_id));
      				    row_id = row_id +1;
      	        		    JSONObject jsonObject1 = (JSONObject) parser.parse(resource.get(i).toString());
      	  				@SuppressWarnings("rawtypes")
				    HashMap map = (HashMap) jsonObject1.get("resource");
      	  			    JSONArray FullnameArray  = (JSONArray) map.get("name");
        		 	    JSONObject nameObject  = (JSONObject) parser.parse(FullnameArray.get(0).toString());
        			    JSONArray nameArray = (JSONArray)(nameObject.get("given"));
        			    System.out.println(nameArray);
        			    if (nameArray.size() == 2) {
    						patientName = (nameArray.get(0) +" "+nameArray.get(1));
    					}
        			    if (nameArray.size() == 1) {
    					patientName = (nameArray.get(0)).toString();
    				    }
        			    if ( map.get("address") != null) {
            			    JSONArray addressArray  = (JSONArray) map.get("address");
            				JSONObject addressObject  = (JSONObject) parser.parse(((JSONArray) map.get("address")).get(0).toString());
                			city = (addressObject.get("city")).toString();
                			state = (addressObject.get("state")).toString();
                			postalCode = (addressObject.get("postalCode")).toString();
        				}
      					put_object.addColumn(FAMILY, P_ID, Bytes.toBytes(map.get("id").toString()));
      	  				put_object.addColumn(FAMILY, BIRTHDATE, Bytes.toBytes(map.get("birthDate").toString()));
      					put_object.addColumn(FAMILY, GENDER, Bytes.toBytes(map.get("gender").toString()));
      					put_object.addColumn(FAMILY, P_ID, Bytes.toBytes(map.get("").toString()));
      					put_object.addColumn(FAMILY, NAME, Bytes.toBytes(patientName));
      					put_object.addColumn(FAMILY, CITY, Bytes.toBytes(city));
      					put_object.addColumn(FAMILY, STATE, Bytes.toBytes(state));
      					put_object.addColumn(FAMILY, POSTALCODE, Bytes.toBytes(postalCode));
					LOGGER.info(put_object.toString());
      					c.output(put_object);
      				}
      			 }
      			catch (Exception e) {
      	        e.printStackTrace();
      	    }
    		}
	};
	


	@SuppressWarnings("unused")
	public static void main(String[] args) 
	{
		// Start by defining the options for the pipeline.
		CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder().withProjectId("healthcare-12").withInstanceId("hc-dataset").withTableId("mihin-dataset").build();
		DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		options.setRunner(BlockingDataflowPipelineRunner.class);
		options.setProject("healthcare-12");
		options.setStagingLocation("gs://mihin-data/staging1");
		Pipeline p = Pipeline.create(options);
 		CloudBigtableIO.initializeForWrite(p);
		/*if(fileFormater.getFile(BUCKET_NAME, "Patient_entry.txt", "PatientFormated.json")){
			LOGGER.info("true");
		     p.apply(TextIO.Read.from("gs://mihin-data/PatientFormated.json")).apply(ParDo.of(Patient_entry.MUTATION_TRANSFORM)).apply(CloudBigtableIO.writeToTable(config));
		     p.run();
			LOGGER.info("pipeline started");
		}
		else{
		LOGGER.info("false");
		}*/
		 p.apply(TextIO.Read.from("gs://mihin-data/temp.json")).apply(ParDo.of(MUTATION_TRANSFORM)).apply(CloudBigtableIO.writeToTable(config));
		     p.run();
		}	
     			
}
