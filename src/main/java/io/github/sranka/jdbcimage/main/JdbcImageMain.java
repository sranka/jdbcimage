package io.github.sranka.jdbcimage.main;

public class JdbcImageMain {
    private static void help() {
        Env.out.println("See documentation at https://github.com/sranka/jdbcimage.");
    }

    public static void main(String... args) throws Exception {
        args = MainToolBase.setupSystemProperties(args);
        String action = args.length > 0 ? args[0] : null;
        if (action != null) {
            String[] restArgs = new String[args.length - 1];
            System.arraycopy(args, 1, restArgs, 0, args.length - 1);

            try {
                switch (action) {
                    case "import":
                        MultiTableConcurrentImport.main(restArgs);
                        break;
                    case "export":
                        MultiTableConcurrentExport.main(restArgs);
                        break;
                    case "dump":
                        TableFileDump.main(restArgs);
                        break;
                    case "dumpHeader":
                        System.setProperty("tool_skip_data", "true");
                        TableFileDump.main(restArgs);
                        break;
                    case "exec":
                        ExecTool.main(restArgs);
                        break;
                    case "help":
                        help();
                        break;
                    default:
                        Env.out.println("Unknown action: " + action);
                        action = null;
                        break;
                }
            } catch (Exception e) {
                Env.exit(1, e);
            }
        }
        if (action == null) {
            help();
            Env.exit(1, null);
        }
    }
}
