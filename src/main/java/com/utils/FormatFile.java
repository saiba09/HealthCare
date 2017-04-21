package com.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;

public class FormatFile {
	
	public StorageObject uploadSimple(Storage storage, String bucketName, String objectName,InputStream data, String contentType) throws IOException {
   		InputStreamContent mediaContent = new InputStreamContent(contentType, data);
   		Storage.Objects.Insert insertObject = storage.objects().insert(bucketName, null, mediaContent).setName(objectName);
   		insertObject.getMediaHttpUploader().setDisableGZipContent(true);
   		return insertObject.execute();
 }
	public boolean getFile(String BUCKET_NAME , String SOURCE_FILE , String DESTINATION_FILE) {
		try{
			HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
       		JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
       		GoogleCredential credential = GoogleCredential.getApplicationDefault();
       		Storage storage = new Storage.Builder(httpTransport, jsonFactory, credential)
       		.setApplicationName("MihinDataFormating/v1").build();
        	Storage.Objects.Get obj = storage.objects().get(BUCKET_NAME, SOURCE_FILE);
        	HttpResponse response = obj.executeMedia();
			String file=response.parseAsString();
			JSONParser parser = new JSONParser();
            Object obj1 = parser.parse(file);
	       	JSONObject jsonObject = (JSONObject) obj1;
	     // result = (jsonObject.toString());
			uploadSimple(storage, BUCKET_NAME, DESTINATION_FILE, new ByteArrayInputStream((jsonObject.toString()).getBytes("UTF-8")), "text/plain");
			//return true;
		}
		catch(Exception e){
			System.out.println(e);
			
		}
		return false;
	}
}
