package pz.tool.jdbcimage.main;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.HashMap;

import pz.tool.jdbcimage.LoggedUtils;
import pz.tool.jdbcimage.ResultConsumer;
import pz.tool.jdbcimage.ResultProducerRunner;
import pz.tool.jdbcimage.ResultSetInfo;
import pz.tool.jdbcimage.RowData;
import pz.tool.jdbcimage.kryo.KryoResultProducer;

/**
 * Perform dump of a specific table file.
 * @author zavora
 *
 */
public class TableFileDump extends MainToolBase{
	// dump file
	public String tool_in_file = System.getProperty("tool_in_file","target/exportMssql/rs_report_def_bundles");
	public String tool_out_file = System.getProperty("tool_out_file","target/rs_report_def_bundles.dump");

	@Override
	protected void initDataSource() {
		// no data source required
	}
	
	public void run() throws Exception{
		File inFile = new File(tool_in_file);
		out.println("Input file: "+inFile);
		File outFile = new File(tool_out_file);
		InputStream in = toResultInput(inFile);
		FileOutputStream _target = new FileOutputStream(outFile);
		PrintStream target = new PrintStream(_target);
		try{
			ResultProducerRunner runner = new ResultProducerRunner(new KryoResultProducer(in), new ResultConsumer<RowData>(){
				private ResultSetInfo info;

				@Override
				public void onStart(ResultSetInfo info) {
					this.info = info;
					target.println("=== COLUMNS =============");
					for(int i=0; i<info.columns.length; i++){
						target.print(info.columns[i]);
						target.print(": ");
						target.print(getTypeName(info.types[i]));
						target.print("\n");
					}
					target.println("=== DATA ================");
				}
				
				@Override
				public void accept(RowData t) {
					for(int i=0; i<t.values.length; i++){
						target.print(info.columns[i]);
						target.print(" ");
						Object value = t.values[i];
						if (value instanceof InputStream){
							InputStream in = (InputStream) value;
							target.flush();
							byte[] chunk = new byte[100];
							int count;
							try{
								while((count = in.read(chunk))!=-1){
									_target.write(chunk,0,count);
								}
								target.println();
							} catch(IOException e){
								throw new RuntimeException(e);
							} finally{
								LoggedUtils.close(in);
							}
						} else{
							target.println(value);
						}
					}
					target.println("-------------------------");
				}
			});
			runner.run();
		} finally{
			LoggedUtils.close(in);
			// close the file
			target.flush();
			LoggedUtils.close(target);
			// do not delete the outFile even if it failed
			out.println("Saved to: "+outFile);
		}
	}
	
	//////////////////////////
	// java Types to String
	//////////////////////////
	private static HashMap<Integer,String> jdbcTypeToName;
	static{
		jdbcTypeToName = new HashMap<>();
		try{
			Field[] fields = java.sql.Types.class.getFields();
			for(int i=0; i<fields.length; i++){
				jdbcTypeToName.put(fields[i].getInt(null),fields[i].getName());
			}
		} catch(Exception e){
			throw new RuntimeException(e);
		}
	}
	
	public static String getTypeName(int jdbcType){
		String val = jdbcTypeToName.get(jdbcType);
		return val == null? String.valueOf(jdbcType):val;
	}
	
	public static void main(String... args) throws Exception{
		try(TableFileDump tool = new TableFileDump()){tool.run();}
	}
}
