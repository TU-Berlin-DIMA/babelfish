package de.tub.dima.babelfish.benchmark;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BenchmarkUtils {

    public static BenchmarkConfig parseConfig(String[] args) {
        return new BenchmarkConfig(args[0], args[1], Boolean.valueOf(args[2]));
    }

    public static BenchmarkConfig parseConfigLayout(String[] args) {
        return new BenchmarkConfig(args[0], args[1], Integer.valueOf(args[2]), Boolean.valueOf(args[3]));
    }

    public static void writeResultFile(String benchmarkName, Object config, BenchmarkResults results) {

        String separator = ",";
        StringBuilder resultString = new StringBuilder();

        String currentTs = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
        resultString.append(currentTs);
        resultString.append(separator);
        Class<? extends Object> configClazz = config.getClass();
        for (Field f : configClazz.getFields()) {
            f.setAccessible(true);
            try {
                resultString.append(f.get(config));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            resultString.append(separator);
        }

        BenchmarkResults.BenchmarkStatistics internal = results.getInternal();
        resultString.append(internal.min);
        resultString.append(separator);
        resultString.append(internal.max);
        resultString.append(separator);
        resultString.append(internal.avg);
        resultString.append(separator);

        BenchmarkResults.BenchmarkStatistics external = results.getExternal();
        resultString.append(external.min);
        resultString.append(separator);
        resultString.append(external.max);
        resultString.append(separator);
        resultString.append(external.avg);
        resultString.append(separator);
        resultString.append("\n");
        try {
            Files.write(
                    Paths.get("results/" + benchmarkName + ".csv"),
                    resultString.toString().getBytes(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeRawResultFile(String benchmarkName, BenchmarkConfig config, BenchmarkResults results) {

        String separator = ",";
        StringBuilder resultString = new StringBuilder();

        for (BenchmarkResults.BenchmarkResult result : results.results) {

            String currentTs = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
            resultString.append(currentTs);
            resultString.append(separator);
            resultString.append(config.query);
            resultString.append(separator);
            resultString.append(config.language);
            resultString.append(separator);
            resultString.append(config.lazy_string_handling);
            resultString.append(separator);
            resultString.append(result.internalTime);
            resultString.append(separator);
            resultString.append(result.externalTime);
            resultString.append("\n");

        }
        try {
            Files.write(
                    Paths.get("results/" + benchmarkName + ".csv"),
                    resultString.toString().getBytes(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
