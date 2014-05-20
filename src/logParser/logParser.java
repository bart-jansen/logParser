package logParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;

import com.json.parsers.JSONParser;
import com.json.parsers.JsonParserFactory;
	
public class logParser {
	public static void main(String[] args) throws IOException {

		File folder = new File("data/");
		File[] listOfFiles = folder.listFiles();

		for (File file : listOfFiles) {
		    if (file.isFile() && !file.getName().contains(".DS_Store")) {
		        System.out.println(file.getName());
		    
		        TarArchiveInputStream myTarFile=new TarArchiveInputStream(new GZIPInputStream(new FileInputStream("data/" + file.getName())));
		
		        TarArchiveEntry entry;
		        int index = 0;
		        int offset;
				BufferedReader perFileBR = null;
				StringBuilder jobJSON = new StringBuilder();
				jobJSON.append("{\"ALLJOBS\": [");
				
				while ((entry = myTarFile.getNextTarEntry()) != null) {
					List<String> Jobs = new ArrayList<String>();
					List<String> Tasks = new ArrayList<String>();
					String fileLine = null;
					
					String JobsJson = "";
		
					if(!entry.getName().contains("_conf") && !entry.isDirectory() && !entry.getName().contains(".crc")) {
		
						perFileBR =	new BufferedReader(new InputStreamReader(myTarFile, "UTF-8"));
						
						while (perFileBR.ready() && (fileLine = perFileBR.readLine()) != null) {
							if(fileLine.length() > 3 && fileLine.substring(0,3).equals("Job")) {
								//System.out.println(jobsToJSON(fileLine)+",");
								
								//Jobs.add(jobsToJSON(fileLine)+",");
								JobsJson += jobsToJSON(fileLine)+",";
							}
							else if(fileLine.length() > 4 && fileLine.substring(0,4).equals("Task")) {
								//System.out.println(taskToJSON(fileLine));
		
								Tasks.add(fileLine);
							}
						    
						}

						if(JobsJson.length() > 1) {	
				        	jobJSON.append("{"+JobsJson.substring(0,JobsJson.length()-1)+"},");
					        
							index++;
		
		
				        }     
				        else { //for error purposes
				        	System.out.println(entry.getName());
				        }
				        
		        		
					}
					
		
				}
				
		    	BufferedWriter writer = new BufferedWriter(new FileWriter("summary/"+file.getName()+".json"));
		
				writer.write(jobJSON.substring(0,jobJSON.length()-1)+"]}");
		        writer.close();
		
				perFileBR.close();
		        myTarFile.close();
		       
		    }	
		}
        
	}
	private static String taskToJSON(String fileLine) {
		return fileLine.replace("=", "\":").replace("\" \"","\"\"").replace("Task TASKID", "{\"TASKID").replace(" .","").replace("\" ", "\",\"");		
	}
	
	private static String jobsToJSON(String fileLine) {
		return fileLine.replace("=", "\":").replace("\" \"","\"\"").replace("Job JOBID", "\"JOBID").replace(" .","").replace("\" ", "\",\"").replace("\\","");		
	}
	
	
}
