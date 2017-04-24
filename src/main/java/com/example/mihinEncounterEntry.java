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
import java.util.ArrayList;
public class mihinEncounterEntry
{
	private static final String BUCKET_NAME = "mihin-data";
	private static final FormatFile fileFormater = new FormatFile();
	private static final Logger LOGGER = Logger.getLogger(mihin.class.getName());
	private static long row_id = 1;    
	private static final byte[] FAMILY = Bytes.toBytes("ColumnFamily1");
	private static final byte[] E_ID = Bytes.toBytes("e_id");
	private static final byte[] KIND = Bytes.toBytes("kind");
	private static final byte[] STARTDATE = Bytes.toBytes("startdate");
	private static final byte[] ENDDATE = Bytes.toBytes("enddate");
	private static final byte[] P_ID = Bytes.toBytes("p_id");
	private static final byte[] STARTTIME = Bytes.toBytes("starttime");
	private static final byte[] ENDTIME = Bytes.toBytes("endtime");

	static final DoFn<String, String> MUTATION_TRANSFORM = new DoFn<String, String>() {   
		private static final long serialVersionUID = 1L;
		@SuppressWarnings("unused")
		@Override
		public void processElement(DoFn<String, String>.ProcessContext c) throws Exception{
			String line = c.element();
			Put put_object = null ;
			int indication_count =0;
			JSONArray indicationObject =null;
			String patientId = null, startDate = null,endDate = null,startTime = null,endTime = null,kind=null,e_id = null ;
			JSONParser parser = new JSONParser();
			try {
				Object obj = parser.parse(line);
				JSONObject jsonObject = (JSONObject) obj;
				JSONArray resource = (JSONArray) jsonObject.get("resources");
				for (int i = 0; i < 50; i++) {
					put_object = new Put(Bytes.toBytes(row_id));
   				    row_id = row_id +1;
					JSONObject jsonObject1 = (JSONObject) parser.parse(resource.get(i).toString());
					HashMap map  = (HashMap) jsonObject1.get("resource");
					HashMap<String , JSONArray> map2  =  (HashMap<String, JSONArray>) jsonObject1.get("resource");
					JSONObject patientObj  =  (JSONObject) map.get("patient");
					String patient =  (String) patientObj.get("reference");
					 patientId = patient.substring(patient.indexOf('/')+1);
					JSONObject periodObj  =  (JSONObject) map.get("period");
					 startDate =  (String) periodObj.get("start");
					 endDate =  (String) periodObj.get("end");
					 startTime = startDate.substring(startDate.indexOf('T')+1);
					startDate = startDate.substring(0,startDate.indexOf("T"));
					 endTime = endDate.substring(endDate.indexOf('T')+1);
					endDate = endDate.substring(0,endDate.indexOf("T"));
					 kind =  (String) map.get("class");
					 e_id = (String) map.get("id");
					if (map2.containsKey("indication")) {
					indicationObject =  (JSONArray) map2.get("indication");
					indication_count = indicationObject.size();
					}
				}
			}
			catch(Exception e){
				e.printStackTrace(); 
				throw e;
			}
			for (int j = 1; j <= indication_count; j++) {
				JSONObject indicationObj = (JSONObject) indicationObject.get(j-1);	  
				String var = "indication"+j;
				String indication = (indicationObj.get("reference").toString());
				indication = (indication.substring(indication.indexOf("/")+1));	
				put_object.addColumn(FAMILY, Bytes.toBytes(var), Bytes.toBytes(indication));

			}          
			put_object.addColumn(FAMILY, E_ID, Bytes.toBytes(e_id));
			put_object.addColumn(FAMILY, STARTDATE, Bytes.toBytes(startDate));
			put_object.addColumn(FAMILY, ENDDATE, Bytes.toBytes(endDate));
			put_object.addColumn(FAMILY, STARTTIME, Bytes.toBytes(startTime));
			put_object.addColumn(FAMILY, P_ID, Bytes.toBytes(patientId));
			put_object.addColumn(FAMILY, ENDTIME, Bytes.toBytes(endTime));
			put_object.addColumn(FAMILY, KIND, Bytes.toBytes(kind));
		//	c.output(put_object);	
			c.output("inserted  : " );

		}
	


};
@SuppressWarnings("unused")
public static void main(String[] args) 
{
	// Start by defining the options for the pipeline.
	CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder().withProjectId("healthcare-12").withInstanceId("hc-dataset").withTableId("mihin-dataset-encounter").build();
	DataflowPipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
	options.setRunner(BlockingDataflowPipelineRunner.class);
	options.setProject("healthcare-12");
	options.setStagingLocation("gs://mihin-data/staging1");
	Pipeline p = Pipeline.create(options);
	CloudBigtableIO.initializeForWrite(p);
	p.apply(TextIO.Read.named("Reading file").from("gs://mihin-data/formatedEncounterEntry.json")).apply(ParDo.named("Cleansing of input data").of(MUTATION_TRANSFORM))
		//.apply(TextIO.Write.to("gs://mihin-data/temp-test-encounter.txt"));
	.apply(CloudBigtableIO.named("Writing to big Table ").writeToTable(config));
	p.run();
}	
}
