package com.dataleading.raft;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpClient {

	private String service;
	private int TIME_OUT = 60*1000;
	
	public HttpClient(String service){
		this.service = service;
	}
		
	public String post(String data) {
		return service("POST", data);
	}
	
	public String get() {
		return service("GET", "");
	}
	
	public HttpClient setTimeOut(int timeout) {
		this.TIME_OUT = timeout;
		return this;
	}
	
	private String service(String method, String data){
		HttpURLConnection conn = null;				
		String response = "";
		
		try {
			URL url = new URL(service);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod(method);

			conn.setConnectTimeout(TIME_OUT);
			conn.setReadTimeout(TIME_OUT);
			conn.setDoOutput(true);
			conn.setUseCaches(false);
			
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("User-Agent","Analytics-WebClient");
			int contLength = data.getBytes("UTF-8").length;
			conn.setRequestProperty("Content-Length", Integer.toString(contLength));
			
			conn.connect();		
			
			if("POST".equals(method)){
				OutputStream os = conn.getOutputStream();
				Writer wr = new OutputStreamWriter(os);
				wr.write(data);
				wr.flush();				
			}

			InputStream stream = null;
			if(conn.getResponseCode()== 200){
				stream = conn.getInputStream();
			}else{
				stream = conn.getErrorStream();
			}
			BufferedReader in = new BufferedReader(new InputStreamReader(stream,"UTF-8"));
			
			StringBuffer buf = new StringBuffer();
			String temp = null;
			while ((temp = in.readLine()) != null) {
				buf.append(temp);
				buf.append("\n");
			}
			response = buf.toString();
			in.close();		
		} catch (IOException e) {
			response = "{\"code\":503,\"message\":\"Connection refused\"}";
			//logger.log(Level.INFO, "failed to call service {0}", service);
			//throw e;
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
		return response;
	}
	
	public String multipartRequest(String fieldName, File uploadFile) throws IOException {
		HttpURLConnection connection = null;
		DataOutputStream outputStream = null;
		InputStream inputStream = null;
		
		String twoHyphens = "--";
		String boundary =  "*****"+Long.toString(System.currentTimeMillis())+"*****";
		String lineEnd = "\r\n";
		
		String result = "";
		
		int bytesRead, bytesAvailable, bufferSize;
		byte[] buffer;
		int maxBufferSize = 1*1024*1024;
		
//		String[] q = filepath.split("/");
//		int idx = q.length - 1;
		
		try {
//			File file = new File(filepath);
			FileInputStream fileInputStream = new FileInputStream(uploadFile);
			
			URL url = new URL(service);
			connection = (HttpURLConnection) url.openConnection();
			
			connection.setDoInput(true);
			connection.setDoOutput(true);
			connection.setUseCaches(false);
			
			connection.setRequestMethod("POST");
			connection.setRequestProperty("Connection", "Keep-Alive");
			connection.setRequestProperty("User-Agent", "Tiny Multipart HTTP Client 1.0");
			connection.setRequestProperty("Content-Type", "multipart/form-data; boundary="+boundary);
			
			outputStream = new DataOutputStream(connection.getOutputStream());
			outputStream.writeBytes(twoHyphens + boundary + lineEnd);
			outputStream.writeBytes("Content-Disposition: form-data; name=\"" + fieldName + "\"; filename=\"" + uploadFile.getName() +"\"" + lineEnd);
			outputStream.writeBytes("Content-Type: image/jpeg" + lineEnd);
			outputStream.writeBytes("Content-Transfer-Encoding: binary" + lineEnd);
			outputStream.writeBytes(lineEnd);
			
			bytesAvailable = fileInputStream.available();
			bufferSize = Math.min(bytesAvailable, maxBufferSize);
			buffer = new byte[bufferSize];
			
			bytesRead = fileInputStream.read(buffer, 0, bufferSize);
			while(bytesRead > 0) {
				outputStream.write(buffer, 0, bufferSize);
				bytesAvailable = fileInputStream.available();
				bufferSize = Math.min(bytesAvailable, maxBufferSize);
				bytesRead = fileInputStream.read(buffer, 0, bufferSize);
			}
			
			outputStream.writeBytes(lineEnd);
			
			outputStream.writeBytes(twoHyphens + boundary + twoHyphens + lineEnd);
			
			inputStream = connection.getInputStream();
			result = this.convertStreamToString(inputStream);
			
			fileInputStream.close();
			inputStream.close();
			outputStream.flush();
			outputStream.close();
			
			return result;
		} catch(Exception e) {
			e.printStackTrace();
			return "error";
		}
	}
	
	private String convertStreamToString(InputStream is) {
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));
		StringBuilder sb = new StringBuilder();

		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				sb.append(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return sb.toString();
	}

}
