package pz.tool.jdbcimage.main;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MultiTableImport extends SingleTableImport{
	
	public void run(){
		long start = System.currentTimeMillis();
		Duration deleteTime = null;
		try {
			List<String> tables = Files.lines(Paths.get(tool_table_file)).collect(Collectors.toList());
			tables.stream().forEach(x -> {
				try{
					out.println(x);
					out.println(" emptied: "+ resetTable(x));
				} catch(Exception e){
					throw new RuntimeException(e);
				}
				
			});
			deleteTime = Duration.ofMillis(System.currentTimeMillis()-start);
			out.println("Total delete time: "+ deleteTime);
			Collections.reverse(tables); // insert in opposite order
			tables.stream().forEach(x -> {
				try{
					out.println(x);
					out.println(" imported: "+ importTable(x));
				} catch(Exception e){
					throw new RuntimeException(e);
				}
				
			});
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		Duration totalTime = Duration.ofMillis(System.currentTimeMillis()-start);
		out.println("Total delete time: "+ deleteTime);
		out.println("Total insert time: "+ totalTime.minus(deleteTime));
		out.println("Total processing time: "+ totalTime);
	}
    
	public static void main(String... args) throws Exception{
		try(MultiTableImport tool = new MultiTableImport()){tool.run();}
	}
}
