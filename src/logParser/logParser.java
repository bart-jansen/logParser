package logParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

	
public class logParser {
	public static void main(String[] args) throws IOException {
		FileReader testFileReader = new FileReader("easylog.log");
		BufferedReader reader = new BufferedReader(testFileReader);
		String line = null;
		
		List<String> Jobs = new ArrayList<String>();
		List<String> Tasks = new ArrayList<String>();

		
		while ((line = reader.readLine()) != null) {
			if(line.substring(0,3).equals("Job")) {
				Jobs.add(line);
			}
			else if(line.substring(0,4).equals("Task")) {
				Tasks.add(line);
			}
			

		}
		
		System.out.println(Jobs.toString());
		System.out.println(Tasks.toString());

		
	}
}
