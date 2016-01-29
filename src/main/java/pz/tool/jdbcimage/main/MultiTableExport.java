package pz.tool.jdbcimage.main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;

public class MultiTableExport extends SingleTableExport{
	
	public void run(){
		long start = System.currentTimeMillis();
		try {
			Files.lines(Paths.get(tool_table_file)).forEach(x -> {
					try{
						out.println(x+": "+ exportTable(x));
					} catch(Exception e){
						throw new RuntimeException(e);
					}
				});
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		out.println("Total processing time: "+ Duration.ofMillis(System.currentTimeMillis()-start));
		out.println("Files saved to: "+new File(tool_builddir));
	}
    
	public static void main(String... args) throws Exception{
		try(MultiTableExport tool = new MultiTableExport()){tool.run();}
	}
}
