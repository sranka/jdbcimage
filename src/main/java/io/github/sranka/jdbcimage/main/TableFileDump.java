package io.github.sranka.jdbcimage.main;

import io.github.sranka.jdbcimage.*;
import io.github.sranka.jdbcimage.kryo.KryoResultProducer;

import java.io.*;
import java.lang.reflect.Field;
import java.util.HashMap;

/**
 * Performs dump of a specific table file.
 * @author zavora
 */
public class TableFileDump extends MainToolBase{
	// dump file
	private String tool_in_file = System.getProperty("tool_in_file",null);
    private final String tool_out_file = System.getProperty("tool_out_file",null);
    private final boolean skipData = Boolean.getBoolean("tool_skip_data");

	@Override
	protected void initDataSource() {
		// no data source required
	}
	
	public void run() throws Exception{
		File inFile = new File(tool_in_file);
		out.println("Input file: "+inFile);
		InputStream in = toResultInput(inFile);
		if (in == null) return; // zip on input
		PrintStream target;
		if (tool_out_file != null && !tool_out_file.isEmpty()){
			File outFile = new File(tool_out_file);
			out.println("Output file: "+outFile);
			FileOutputStream _target = new FileOutputStream(outFile);
			target = new PrintStream(_target);
		} else{
			target = out;
		}
		try{
			ResultProducerRunner runner = new ResultProducerRunner(new KryoResultProducer(in), new ResultConsumer<RowData>(){
				private ResultSetInfo info;
                private long rows;

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
                    rows++;
					if (!skipData) {
						for (int i = 0; i < t.values.length; i++) {
							target.print(info.columns[i]);
							target.print(" ");
							Object value = t.values[i];
							if (value instanceof byte[]) value = new ByteArrayInputStream((byte[]) value);
							if (value instanceof InputStream) {
								InputStream in = (InputStream) value;
								target.flush();
								byte[] chunk = new byte[100];
								int count;
								try {
									while ((count = in.read(chunk)) != -1) {
										target.write(chunk, 0, count);
									}
									target.println();
								} catch (IOException e) {
									throw new RuntimeException(e);
								} finally {
									LoggedUtils.close(in);
								}
							} else if (value instanceof Reader) {
								Reader in = (Reader) value;
								target.flush();
								char[] chunk = new char[100];
								int count;
								try {
									while ((count = in.read(chunk)) != -1) {
										target.print(new String(chunk, 0, count));
									}
									target.println();
								} catch (IOException e) {
									throw new RuntimeException(e);
								} finally {
									LoggedUtils.close(in);
								}
							} else {
								target.println(value);
							}
						}
						target.println("-------------------------");
					}
				}

                @Override
                public long onFinish() {
                    target.println("Records processed - "+rows);
                    return rows;
                }
            });
			runner.run();
		} finally{
			LoggedUtils.close(in);
			// close the file
			target.flush();
			if (target!=out){
				LoggedUtils.close(target);
			}
			// do not delete the outFile even if it failed
		}
	}
	
	//////////////////////////
	// java Types to String
	//////////////////////////
	private static final HashMap<Integer,String> jdbcTypeToName;
	static{
		jdbcTypeToName = new HashMap<>();
		try{
			Field[] fields = java.sql.Types.class.getFields();
			for (Field field : fields) {
				jdbcTypeToName.put(field.getInt(null), field.getName());
			}
		} catch(Exception e){
			throw new RuntimeException(e);
		}
	}
	
	private static String getTypeName(int jdbcType){
		String val = jdbcTypeToName.get(jdbcType);
		return val == null? String.valueOf(jdbcType):val;
	}
	
	public static void main(String... args) throws Exception{
		args = setupSystemProperties(args);

		try(TableFileDump tool = new TableFileDump()){
			if (tool.tool_in_file == null || tool.tool_in_file.isEmpty()) {
				if (args.length == 0 || args[0].isEmpty()) {
					throw new IllegalArgumentException("Expected file as an argument, but no or empty argument supplied!");
				} else{
					tool.tool_in_file = args[0];
				}
			}
			tool.run();
		} catch(IllegalArgumentException e){
			System.out.println("FAILED: "+e.getMessage());
			System.exit(1);
		}
	}
}
