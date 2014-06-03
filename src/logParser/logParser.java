package logParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import com.json.parsers.JSONParser;
import com.json.parsers.JsonParserFactory;
	
public class logParser {
	public static void main(String[] args) throws IOException {
		
		File folder = new File("data/");
		File[] listOfFiles = folder.listFiles();

		//loop through every file in above specified directory
		for (File file : listOfFiles) {
			//ignoring directories & files containing .DS_Store
		    if (file.isFile() && !file.getName().contains(".DS_Store")) {
		        System.out.println(file.getName());
		    
		        //open found .tar.gz file
		        TarArchiveInputStream myTarFile=new TarArchiveInputStream(new GZIPInputStream(new FileInputStream("data/" + file.getName())));
		
		        TarArchiveEntry entry;
		        int index = 0;
				BufferedReader perFileBR = null;
				StringBuilder jobJSON = new StringBuilder();
				jobJSON.append("{\"ALLJOBS\": [");
				
				//loop through all files, compressed within specified .tar.gz archive 
				while ((entry = myTarFile.getNextTarEntry()) != null) {
					List<String> Tasks = new ArrayList<String>();
					String fileLine = null;
					
					String returnedJSON = "";
					String JobsJson = "";
					
					//ignore _conf.xml & .crc files, since we're only interested in jobtracker logs
					if(!entry.getName().contains("_conf") && !entry.isDirectory() && !entry.getName().contains(".crc")) {
							
						perFileBR =	new BufferedReader(new InputStreamReader(myTarFile, "UTF-8"));
						
						//looping through every line of the jobtracker log
						while (perFileBR.ready() && (fileLine = perFileBR.readLine()) != null) {
							
							//looking for the JOB-related logs in the jobtracker log
							if(fileLine.length() > 3 && fileLine.substring(0,3).equals("Job")) {
								//System.out.println(jobsToJSON(fileLine)+",");
								
								//converting job log line to JSON
								returnedJSON = jobsToJSON(fileLine);
								
								if(returnedJSON.contains("\"MAP_COUNTERS\":\"")) {
									JobsJson += returnedJSON.substring(0,returnedJSON.indexOf("\"MAP_COUNTERS\":\""));
									
									JobsJson += convertCountersToJSON(returnedJSON.substring(returnedJSON.indexOf("\"MAP_COUNTERS\":\""), returnedJSON.indexOf("\"REDUCE_COUNTERS\":\"")), "MAP_COUNTERS");
									JobsJson += convertCountersToJSON(returnedJSON.substring(returnedJSON.indexOf("\"REDUCE_COUNTERS\":\""), returnedJSON.indexOf("\"COUNTERS\":\"")), "REDUCE_COUNTERS");
									JobsJson += convertCountersToJSON(returnedJSON.substring(returnedJSON.indexOf("\"COUNTERS\":\"")), "COUNTERS");

								}
								else {
									JobsJson += returnedJSON +",";
								}
							}
							/*else if(fileLine.length() > 4 && fileLine.substring(0,4).equals("Task")) {
								//System.out.println(taskToJSON(fileLine));
		
								Tasks.add(fileLine);
							}*/
						    
						}

						if(JobsJson.length() > 1) {	
							//appending all jobJSON logs for the selected archive
				        	jobJSON.append("{"+JobsJson.substring(0,JobsJson.length()-1)+"},");
					        
							index++;
		
		
				        }     
				        else { //for error purposes
				        	System.out.println(entry.getName());
				        }
				        
		        		
					}
					
					index++;
				}
				
				//writing the jobJSON to summary/+date+.json file
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
	
	private static String convertCountersToJSON(String counter, String jsonName) {
		//String testString = "Job JOBID=\"job_201404091337_6885\" FINISH_TIME=\"1397779206354\" JOB_STATUS=\"SUCCESS\" FINISHED_MAPS=\"50\" FINISHED_REDUCES=\"1\" FAILED_MAPS="0" FAILED_REDUCES="0" MAP_COUNTERS="{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_READ)(HDFS_BYTES_READ)(820607541)][(FILE_BYTES_WRITTEN)(FILE_BYTES_WRITTEN)(188545292)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(MAP_INPUT_RECORDS)(Map input records)(50)][(PHYSICAL_MEMORY_BYTES)(Physical memory (bytes) snapshot)(53227896832)][(SPILLED_RECORDS)(Spilled Records)(7500)][(MAP_OUTPUT_BYTES)(Map output bytes)(522925200)][(COMMITTED_HEAP_BYTES)(Total committed heap usage (bytes))(80981852160)][(CPU_MILLISECONDS)(CPU time spent (ms))(1363400)][(VIRTUAL_MEMORY_BYTES)(Virtual memory (bytes) snapshot)(241794482176)][(SPLIT_RAW_BYTES)(SPLIT_RAW_BYTES)(639418)][(MAP_OUTPUT_RECORDS)(Map output records)(7500)][(COMBINE_INPUT_RECORDS)(Combine input records)(0)]}" REDUCE_COUNTERS="{(FileSystemCounters)(FileSystemCounters)[(FILE_BYTES_READ)(FILE_BYTES_READ)(184454873)][(HDFS_BYTES_READ)(HDFS_BYTES_READ)(73033350)][(FILE_BYTES_WRITTEN)(FILE_BYTES_WRITTEN)(184539460)][(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(58643916)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(REDUCE_INPUT_GROUPS)(Reduce input groups)(7500)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(184310527)][(PHYSICAL_MEMORY_BYTES)(Physical memory (bytes) snapshot)(2276683776)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(0)][(SPILLED_RECORDS)(Spilled Records)(7500)][(COMMITTED_HEAP_BYTES)(Total committed heap usage (bytes))(2262958080)][(CPU_MILLISECONDS)(CPU time spent (ms))(119700)][(VIRTUAL_MEMORY_BYTES)(Virtual memory (bytes) snapshot)(4872900608)][(COMBINE_INPUT_RECORDS)(Combine input records)(0)][(REDUCE_INPUT_RECORDS)(Reduce input records)(7500)]}" COUNTERS="{(org.apache.hadoop.mapred.JobInProgress$Counter)(Job Counters )[(SLOTS_MILLIS_MAPS)(SLOTS_MILLIS_MAPS)(1240450)][(TOTAL_LAUNCHED_REDUCES)(Launched reduce tasks)(1)][(FALLOW_SLOTS_MILLIS_REDUCES)(Total time spent by all reduces waiting after reserving slots (ms))(0)][(RACK_LOCAL_MAPS)(Rack-local map tasks)(36)][(FALLOW_SLOTS_MILLIS_MAPS)(Total time spent by all maps waiting after reserving slots (ms))(0)][(TOTAL_LAUNCHED_MAPS)(Launched map tasks)(50)][(DATA_LOCAL_MAPS)(Data-local map tasks)(2)][(SLOTS_MILLIS_REDUCES)(SLOTS_MILLIS_REDUCES)(77820)]}{(FileSystemCounters)(FileSystemCounters)[(FILE_BYTES_READ)(FILE_BYTES_READ)(184454873)][(HDFS_BYTES_READ)(HDFS_BYTES_READ)(893640891)][(FILE_BYTES_WRITTEN)(FILE_BYTES_WRITTEN)(373084752)][(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(58643916)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(50)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(184310527)][(SPILLED_RECORDS)(Spilled Records)(15000)][(MAP_OUTPUT_BYTES)(Map output bytes)(522925200)][(CPU_MILLISECONDS)(CPU time spent (ms))(1483100)][(COMMITTED_HEAP_BYTES)(Total committed heap usage (bytes))(83244810240)][(COMBINE_INPUT_RECORDS)(Combine input records)(0)][(SPLIT_RAW_BYTES)(SPLIT_RAW_BYTES)(639418)][(REDUCE_INPUT_RECORDS)(Reduce input records)(7500)][(REDUCE_INPUT_GROUPS)(Reduce input groups)(7500)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(PHYSICAL_MEMORY_BYTES)(Physical memory (bytes) snapshot)(55504580608)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(0)][(VIRTUAL_MEMORY_BYTES)(Virtual memory (bytes) snapshot)(246667382784)][(MAP_OUTPUT_RECORDS)(Map output records)(7500)]}"";
		
		//String testString = "JOB JOEOHOE =\"test\" ";
		//regex "COUNTER="(.*?)""
		
		String counterRegex = "^[.]*[\\[][\\(]([^\\)]*)[\\]\\)\\[\\(]*[^\\)]*[\\]\\)\\(]*([^\\)]*)[\\]\\)]*(.*)";
		
		counter = counter.replace("(ms)", "");
		counter = counter.replace("(bytes)", "");

		 String[] counterArray = counter.split("\\[");
		 //System.out.println(Arrays.toString(counterArray));
		 String returnData = "\""+jsonName+"\": { ";
		 
		 for( int i = 1; i < counterArray.length - 1; i++) {
			 Matcher m = Pattern.compile(counterRegex)
				     .matcher("[" + counterArray[i]);
				 
			 m.find();
			 returnData += "\"" + m.group(1) + "\":" + m.group(2) + ",";
		}
		 
		return returnData.substring(0, returnData.length()-1)+"},";
	}
	
	
}
