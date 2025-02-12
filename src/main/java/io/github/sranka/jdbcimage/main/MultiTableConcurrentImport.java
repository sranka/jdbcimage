package io.github.sranka.jdbcimage.main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * DB import that runs in multiple threads.
 */
public class MultiTableConcurrentImport extends SingleTableImport {
    private final boolean tool_disableIndexes = Boolean.parseBoolean(System.getProperty("tool_disableIndexes", "false"));
    private final EnumMap<Step, Boolean> enabledSteps = new EnumMap<>(Step.class);

    public MultiTableConcurrentImport(String steps) {
        super();
        if (steps == null) {
            // enable
            Stream.of(Step.class.getEnumConstants()).forEach(x -> enabledSteps.put(x, true));
        } else {
            Stream.of(Step.class.getEnumConstants()).forEach(x -> enabledSteps.put(x, false));
            Stream.of(steps.split(",")).map(String::trim).filter(x -> !x.isEmpty()).forEach(x -> {
                if (x.startsWith("not-")) {
                    Stream.of(Step.class.getEnumConstants()).forEach(s -> enabledSteps.put(s, true));
                    enabledSteps.put(Step.valueOf(x.substring("not-".length())), Boolean.FALSE);
                } else {
                    enabledSteps.put(Step.valueOf(x), Boolean.TRUE);
                }
            });
        }
    }

    public static void main(String... args) throws Exception {
        args = setupSystemProperties(args);

        try (MultiTableConcurrentImport tool = new MultiTableConcurrentImport(System.getProperty("steps"))) {
            tool.setupZipFile(args);
            tool.run();
        }
    }

    /**
     * Main execution point.
     */
    public void run() throws SQLException, IOException {
        // print platform concurrency, just FYI
        out.println("Concurrency: " + concurrency);
        unzip(); // unzip input if it exists

        Durations durations = new Durations();
        try {
            List<String> dbTables = getUserTables();
            Map<String, String> dbTablesMap = new HashMap<>();
            dbTables.forEach(x -> dbTablesMap.put(x.toLowerCase(), x));
            Map<String, String> conflictingFiles = new HashMap<>();
            // collect tables to import (ignore tables that do not exist)
            try (Stream<Path> fileStream = Files.list(Paths.get(getBuildDirectory().toString()))) {
                setTables(fileStream
                        .filter(x -> {
                            File f = x.toFile();
                            return f.isFile() && !f.getName().contains(".");
                        })
                        .collect(
                                LinkedHashMap<String, String>::new,
                                (map, x) -> {
                                    String fileName = x.getFileName().toString();
                                    String lowerCaseTableName = fileName.toLowerCase();
                                    String retVal = dbTablesMap.get(lowerCaseTableName);
                                    if (retVal == null) {
                                        out.println("SKIPPED - table " + x + " does not exists!");
                                    } else {
                                        map.put(retVal, fileName);
                                    }
                                    String previousFile = conflictingFiles.put(lowerCaseTableName, fileName);
                                    if (previousFile != null) {
                                        throw new RuntimeException("Unsupported data on input. Only one files must describe a case-sensitive table, but found " + previousFile + " and " + fileName);
                                    }
                                },
                                LinkedHashMap::putAll
                        ), out);
            }
            if (!tables.isEmpty()) {
                // apply a procedure that ignores indexes and constraints
                // to speed up data import

                long time;
                // 0. import started
                if (enabledSteps.get(Step.importStarted)) dbFacade.importStarted();
                // 1. disable constraints
                time = System.currentTimeMillis();
                if (enabledSteps.get(Step.disableConstraints)) dbFacade.modifyConstraints(false);
                durations.disableConstraints = Duration.ofMillis(System.currentTimeMillis() - time);
                // 2. make indexes unusable skipped
                if (enabledSteps.get(Step.disableIndexes) && tool_disableIndexes) {
                    time = System.currentTimeMillis();
                    dbFacade.modifyIndexes(false);
                    durations.disableIndexes = Duration.ofMillis(System.currentTimeMillis() - time);
                }
                // 3. delete data
                time = System.currentTimeMillis();
                if (enabledSteps.get(Step.deleteData)) deleteData();
                durations.deleteData = Duration.ofMillis(System.currentTimeMillis() - time);
                // 4. do import
                time = System.currentTimeMillis();
                if (enabledSteps.get(Step.importData)) importData();
                durations.importData = Duration.ofMillis(System.currentTimeMillis() - time);
                // 5. rebuild indexes
                if (enabledSteps.get(Step.enableIndexes) && tool_disableIndexes) {
                    time = System.currentTimeMillis();
                    dbFacade.modifyIndexes(true);
                    durations.enableIndexes = Duration.ofMillis(System.currentTimeMillis() - time);
                }
                // 6. enable constraints
                time = System.currentTimeMillis();
                if (enabledSteps.get(Step.enableConstraints)) dbFacade.modifyConstraints(true);
                durations.enableConstraints = Duration.ofMillis(System.currentTimeMillis() - time);
                // 7. finished
                if (enabledSteps.get(Step.importFinished)) dbFacade.importFinished();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        out.println("Disable constraints time: " + durations.disableConstraints);
        if (tool_disableIndexes) out.println("Disable indexes time: " + durations.disableIndexes);
        out.println("Delete data time: " + durations.deleteData);
        out.println("Import data time: " + durations.importData);
        if (tool_disableIndexes) out.println("Enable indexes time: " + durations.enableIndexes);
        out.println("Enable constraints time: " + durations.enableConstraints);
    }

    private void deleteData() {
        List<Callable<?>> tasks = new ArrayList<>(tables.size());
        for (String table : tables.keySet()) {
            tasks.add(() -> {
                boolean failed = true;
                try {
                    truncateTable(table);
                    out.println("SUCCESS: Truncated table " + table);
                    failed = false;
                } finally {
                    if (failed) {
                        out.println("FAILURE: Truncate table " + table);
                    }
                }
                return null;
            });
        }
        run(tasks);
    }

    private void importData() {
        List<Callable<?>> tasks = new ArrayList<>(tables.size());
        for (Map.Entry<String, String> entry : tables.entrySet()) {
            String table = entry.getKey();
            String fileName = entry.getValue();
            tasks.add(() -> {
                boolean failed = true;
                try {
                    long start = System.currentTimeMillis();
                    long rows = importTable(table, new File(getBuildDirectory(), fileName), dbFacade.getTableInfo(table));
                    out.println("SUCCESS: Imported data to " + table + " - " + rows + " rows in " + Duration.ofMillis(System.currentTimeMillis() - start));
                    failed = false;
                } finally {
                    if (failed) {
                        out.println("FAILURE: Import data to table " + table);
                    }
                }
                return null;
            });
        }
        run(tasks);
    }

    private enum Step {
        importStarted,
        disableConstraints,
        disableIndexes,
        deleteData,
        importData,
        enableIndexes,
        enableConstraints,
        importFinished
    }

    /**
     * Durations to print out at the end.
     */
    private static class Durations {
        Duration disableConstraints = null;
        Duration disableIndexes = null;
        Duration enableConstraints = null;
        Duration enableIndexes = null;
        Duration deleteData = null;
        Duration importData = null;
    }
}