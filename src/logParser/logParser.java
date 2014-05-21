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
		/* for test purposes */
		String dummyCounter ="{(org.apache.hadoop.mapred.JobInProgress$Counter)(Job Counters )[(SLOTS_MILLIS_MAPS)(SLOTS_MILLIS_MAPS)(41238)][(TOTAL_LAUNCHED_REDUCES)(Launched reduce tasks)(1)][(FALLOW_SLOTS_MILLIS_REDUCES)(Total time spent by all reduces waiting after reserving slots (ms))(0)][(FALLOW_SLOTS_MILLIS_MAPS)(Total time spent by all maps waiting after reserving slots (ms))(0)][(TOTAL_LAUNCHED_MAPS)(Launched map tasks)(4)][(DATA_LOCAL_MAPS)(Data-local map tasks)(4)][(SLOTS_MILLIS_REDUCES)(SLOTS_MILLIS_REDUCES)(90092)]}{(FileSystemCounters)(FileSystemCounters)[(FILE_BYTES_READ)(FILE_BYTES_READ)(228945627)][(HDFS_BYTES_READ)(HDFS_BYTES_READ)(62302007)][(FILE_BYTES_WRITTEN)(FILE_BYTES_WRITTEN)(351933997)][(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(58589293)]}{(org.apache.hadoop.mapred.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(145489)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(122678795)][(SPILLED_RECORDS)(Spilled Records)(415242)][(MAP_OUTPUT_BYTES)(Map output bytes)(440301907)][(CPU_MILLISECONDS)(CPU time spent (ms))(135160)][(COMMITTED_HEAP_BYTES)(Total committed heap usage (bytes))(5427036160)][(COMBINE_INPUT_RECORDS)(Combine input records)(0)][(SPLIT_RAW_BYTES)(SPLIT_RAW_BYTES)(592)][(REDUCE_INPUT_RECORDS)(Reduce input records)(144898)][(REDUCE_INPUT_GROUPS)(Reduce input groups)(133396)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(PHYSICAL_MEMORY_BYTES)(Physical memory (bytes) snapshot)(2613743616)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(133396)][(VIRTUAL_MEMORY_BYTES)(Virtual memory (bytes) snapshot)(13456371712)][(MAP_OUTPUT_RECORDS)(Map output records)(144898)]}";
		convertCountersToJSON(dummyCounter);
		
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
								JobsJson += jobsToJSON(fileLine)+",";
							}
							else if(fileLine.length() > 4 && fileLine.substring(0,4).equals("Task")) {
								//System.out.println(taskToJSON(fileLine));
		
								Tasks.add(fileLine);
							}
						    
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
	
	/* WORK IN PROGRESS */
	private static String convertCountersToJSON(String counter) {
		//String testString = 'Job JOBID="job_201404091337_6885" FINISH_TIME="1397779206354" JOB_STATUS="SUCCESS" FINISHED_MAPS="50" FINISHED_REDUCES="1" FAILED_MAPS="0" FAILED_REDUCES="0" MAP_COUNTERS="{(FileSystemCounters)(FileSystemCounters)[(HDFS_BYTES_READ)(HDFS_BYTES_READ)(820607541)][(FILE_BYTES_WRITTEN)(FILE_BYTES_WRITTEN)(188545292)]}{(org\.apache\.hadoop\.mapred\.Task$Counter)(Map-Reduce Framework)[(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(MAP_INPUT_RECORDS)(Map input records)(50)][(PHYSICAL_MEMORY_BYTES)(Physical memory \\(bytes\\) snapshot)(53227896832)][(SPILLED_RECORDS)(Spilled Records)(7500)][(MAP_OUTPUT_BYTES)(Map output bytes)(522925200)][(COMMITTED_HEAP_BYTES)(Total committed heap usage \\(bytes\\))(80981852160)][(CPU_MILLISECONDS)(CPU time spent \\(ms\\))(1363400)][(VIRTUAL_MEMORY_BYTES)(Virtual memory \\(bytes\\) snapshot)(241794482176)][(SPLIT_RAW_BYTES)(SPLIT_RAW_BYTES)(639418)][(MAP_OUTPUT_RECORDS)(Map output records)(7500)][(COMBINE_INPUT_RECORDS)(Combine input records)(0)]}" REDUCE_COUNTERS="{(FileSystemCounters)(FileSystemCounters)[(FILE_BYTES_READ)(FILE_BYTES_READ)(184454873)][(HDFS_BYTES_READ)(HDFS_BYTES_READ)(73033350)][(FILE_BYTES_WRITTEN)(FILE_BYTES_WRITTEN)(184539460)][(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(58643916)]}{(org\.apache\.hadoop\.mapred\.Task$Counter)(Map-Reduce Framework)[(REDUCE_INPUT_GROUPS)(Reduce input groups)(7500)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(184310527)][(PHYSICAL_MEMORY_BYTES)(Physical memory \\(bytes\\) snapshot)(2276683776)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(0)][(SPILLED_RECORDS)(Spilled Records)(7500)][(COMMITTED_HEAP_BYTES)(Total committed heap usage \\(bytes\\))(2262958080)][(CPU_MILLISECONDS)(CPU time spent \\(ms\\))(119700)][(VIRTUAL_MEMORY_BYTES)(Virtual memory \\(bytes\\) snapshot)(4872900608)][(COMBINE_INPUT_RECORDS)(Combine input records)(0)][(REDUCE_INPUT_RECORDS)(Reduce input records)(7500)]}" COUNTERS="{(org\.apache\.hadoop\.mapred\.JobInProgress$Counter)(Job Counters )[(SLOTS_MILLIS_MAPS)(SLOTS_MILLIS_MAPS)(1240450)][(TOTAL_LAUNCHED_REDUCES)(Launched reduce tasks)(1)][(FALLOW_SLOTS_MILLIS_REDUCES)(Total time spent by all reduces waiting after reserving slots \\(ms\\))(0)][(RACK_LOCAL_MAPS)(Rack-local map tasks)(36)][(FALLOW_SLOTS_MILLIS_MAPS)(Total time spent by all maps waiting after reserving slots \\(ms\\))(0)][(TOTAL_LAUNCHED_MAPS)(Launched map tasks)(50)][(DATA_LOCAL_MAPS)(Data-local map tasks)(2)][(SLOTS_MILLIS_REDUCES)(SLOTS_MILLIS_REDUCES)(77820)]}{(FileSystemCounters)(FileSystemCounters)[(FILE_BYTES_READ)(FILE_BYTES_READ)(184454873)][(HDFS_BYTES_READ)(HDFS_BYTES_READ)(893640891)][(FILE_BYTES_WRITTEN)(FILE_BYTES_WRITTEN)(373084752)][(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(58643916)]}{(org\.apache\.hadoop\.mapred\.Task$Counter)(Map-Reduce Framework)[(MAP_INPUT_RECORDS)(Map input records)(50)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(184310527)][(SPILLED_RECORDS)(Spilled Records)(15000)][(MAP_OUTPUT_BYTES)(Map output bytes)(522925200)][(CPU_MILLISECONDS)(CPU time spent \\(ms\\))(1483100)][(COMMITTED_HEAP_BYTES)(Total committed heap usage \\(bytes\\))(83244810240)][(COMBINE_INPUT_RECORDS)(Combine input records)(0)][(SPLIT_RAW_BYTES)(SPLIT_RAW_BYTES)(639418)][(REDUCE_INPUT_RECORDS)(Reduce input records)(7500)][(REDUCE_INPUT_GROUPS)(Reduce input groups)(7500)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(PHYSICAL_MEMORY_BYTES)(Physical memory \\(bytes\\) snapshot)(55504580608)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(0)][(VIRTUAL_MEMORY_BYTES)(Virtual memory \\(bytes\\) snapshot)(246667382784)][(MAP_OUTPUT_RECORDS)(Map output records)(7500)]}"';
		//regex "COUNTER="(.*?)""
		
		String counterRegex = "^[.]*[\\[][\\(]([^\\)]*)[\\]\\)\\[\\(]*[^\\)]*[\\]\\)\\(]*([^\\)]*)[\\]\\)]*(.*)";
		
		counter = counter.replace("(ms)", "");
		counter = counter.replace("(bytes)", "");

		 String[] counterArray = counter.split("\\[");
		 //System.out.println(Arrays.toString(counterArray));
		 
		 for( int i = 1; i < counterArray.length - 1; i++) {
			 Matcher m = Pattern.compile(counterRegex)
				     .matcher("[" + counterArray[i]);
				 
			 m.find();
			 System.out.println(m.group(1));
			 System.out.println(m.group(2));
		}
		 
		 return "test";
	}
	
	
}
