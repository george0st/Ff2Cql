package org.george0st;

import com.opencsv.exceptions.CsvValidationException;
import org.george0st.helper.ExitCodes;
import org.george0st.helper.ReadableValue;
import org.george0st.helper.Setup;
import org.george0st.processor.CsvCqlWrite;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;


@Command(name = "example", mixinStandardHelpOptions = true, version = "FfCsv2Cql 1.2", description = "Simple transfer data from NiFi FileFlow to CQL.")
public class Main implements Callable<Integer> {

    @Option(names = { "-c", "--config" },
            description = "Config file (default is 'connection.json').",
            defaultValue = "connection-private.json")
    private String config = "connection-private.json";

    @Option(names = { "-b", "--bulk" },
            description = "Bulk size (default is 200).", defaultValue = "200")
    private long bulk = 200;

    @Option(names = { "-d", "--dryRun" },
            description = "Dry run, whole processing without write to CQL.")
    private boolean dryRun = false;

    @Option(names = { "-s", "--stdIn" },
            description = "Use input from stdin.")
    private boolean stdIn = false;

    @Option(names = { "-e", "--errorStop" },
            description = "Stop processing in case an error.")
    private boolean errorStop = false;

    @Parameters(arity = "0..*", paramLabel = "INPUT", description = "Input file(s) for processing (optional in case '-s').")
    private String[] inputFiles=null;

    private Integer callCore(CqlAccess access, String inputFile) {

        try {
            long finish, start, count;

            start = System.currentTimeMillis();
            CsvCqlWrite write = new CsvCqlWrite(access, dryRun);
            count = write.execute(inputFile);
            finish = System.currentTimeMillis();

            //  print processing detail
            System.out.println("'" + inputFile + "': " + ReadableValue.fromMillisecond(finish - start) +
                    "(" + (finish - start) + " ms), " +
                    "Items: " + count + ", " +
                    String.format("Perf: %d [calls/sec], ", count / ((finish - start) / 1000)));
        }
        catch (CsvValidationException ex){
            logger.error(String.format("CSV error '%s', exception '%s'.", inputFile, ex));
            if (errorStop)
                return ExitCodes.CSV_ERROR;
        }
        catch(Exception ex) {
            logger.error(String.format("Processing error '%s', exception '%s'.", inputFile, ex));
            if (errorStop)
                return ExitCodes.PROCESSING_ERROR;
        }
        return ExitCodes.SUCCESS;
    }

    @Override
    public Integer call() {
        try {

            //  check parameters
            if (!stdIn)
                if ((inputFiles==null) || (inputFiles.length==0)){
                    logger.error(String.format("Missing parameters 'INPUT' or parameter '-s'."));
                    return ExitCodes.PARAMETR_ERROR;
            }

            // define setup
            Setup setup = Setup.getInstance(config);
            setup.setBulk(bulk);

            // general access
            CqlAccess access=new CqlAccess(setup);
//            long finish, start, count;

            //  main processing
            if (stdIn)
                callCore(access, null);
            else
                for (String inputFile : inputFiles)
                    callCore(access, inputFile);

//                try {
//                    //  write file
//                    start = System.currentTimeMillis();
//                    CsvCqlWrite write = new CsvCqlWrite(access, dryRun);
//                    count = write.execute(inputFile);
//                    finish = System.currentTimeMillis();
//
//                    //  print processing detail
//                    System.out.println("'" + inputFile + "': " + ReadableValue.fromMillisecond(finish - start) +
//                            "(" + (finish - start) + " ms), " +
//                            "Items: " + count + ", " +
//                            String.format("Perf: %d [calls/sec], ", count/((finish-start)/1000)));
//                }
//                catch (CsvValidationException ex){
//                    logger.error(String.format("CSV error '%s', exception '%s'.", inputFile, ex));
//                    if (errorStop)
//                        return ExitCodes.CSV_ERROR;
//                }
//                catch(Exception ex) {
//                    logger.error(String.format("Processing error '%s', exception '%s'.", inputFile, ex));
//                    if (errorStop)
//                        return ExitCodes.PROCESSING_ERROR;
//                }
        }
        catch(IOException ex) {
            logger.error(String.format("Config error '%s'",ex));
            return ExitCodes.CONFIG_ERROR;
        }
        catch(Exception ex) {
            logger.error(String.format("General error '%s'",ex));
            return ExitCodes.GENERAL_ERROR;
        }
        return ExitCodes.SUCCESS;
    }

    public static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static String getInput() throws IOException {
        System.out.println("stdin-start");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
            sb.append(line);
        }
        System.out.println("stdin-stop");
        return sb.toString();
        //return reader.lines().collect(Collectors.joining("\n"));
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

}