package pz.tool.jdbcimage.main;

public class JdbcImageMain {
    private static void help(){
        System.out.println("See documentation at https://github.com/sranka/jdbc-image-tool");
    }
    public static void main(String... args)  throws Exception{
        args = MainToolBase.setupSystemProperties(args);
        String action = args.length>0?args[0]:null;
        if (action!=null) {
            String[] restArgs = new String[args.length-1];
            System.arraycopy(args, 1, restArgs,0,args.length-1);

            try {
                if ("import".equals(action)) {
                    MultiTableParallelImport.main(restArgs);
                } else if ("export".equals(action)) {
                    MultiTableParallelExport.main(restArgs);
                } else if ("dump".equals(action)) {
                    TableFileDump.main(restArgs);
                } else if ("dumpHeader".equals(action)) {
                    System.setProperty("tool_skip_data", "true");
                    TableFileDump.main(restArgs);
                } else if ("exec".equals(action)) {
                    ExecTool.main(restArgs);
                } else {
                    System.out.println("Unknown action: " + action);
                    action = null;
                }
            } catch (Exception e){
                e.printStackTrace();
                System.exit(1);
            }
        }
        if (action == null){
            help();
            System.exit(1);
        }
    }
}
