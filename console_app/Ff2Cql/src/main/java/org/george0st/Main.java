package org.george0st;

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


@Command(name = "example",
        mixinStandardHelpOptions = true,
        version = "Ff2Cql 1.7",
        description = "Simple transfer data from NiFi FlowFile to CQL.")
public class Main implements Callable<Integer> {

    @Option(names = { "-c", "--config" },
            description = "Config file (default is 'connection.json').",
            defaultValue = "connection.json")
    private String config = "connection.json";

    @Option(names = { "-b", "--batch" },
            description = "Batch size for mass upserts (default is 200).", defaultValue = "200")
    private long batch = 200;

    @Option(names = { "-d", "--dryRun" },
            description = "Dry run, whole processing without write to CQL.")
    private boolean dryRun = false;

    @Option(names = { "-s", "--stdIn" },
            description = "Use input from stdin (without 'INPUT' file(s)).")
    private boolean stdIn = false;

    @Option(names = { "-e", "--errorStop" },
            description = "Stop processing in case an error.")
    private boolean errorStop = false;

    @Parameters(arity = "0..*", paramLabel = "INPUT", description = "Input file(s) for processing (optional in case '-s').")
    private String[] inputFiles=null;

    /**
     * Processing *.csv file or stdin stream.
     * @param access    Access for CQL
     * @param inputFile Specific input file (*.csv) or null (for processing stdin)
     * @return Relevant exit code
     */
    private Integer callCore(CqlAccess access, String inputFile) {
        try {
            long finish, start, count;

            //  main processing
            start = System.currentTimeMillis();
            CsvCqlWrite write = new CsvCqlWrite(access, dryRun);
            count = write.execute(inputFile);
            finish = System.currentTimeMillis();

            //  print processing detail
            System.out.printf("'%s': %s (%d ms), Items: %d, Perf: %d [calls/sec]%s",
                    inputFile == null ? "stdin" : inputFile,
                    ReadableValue.fromMillisecond(finish - start),
                    finish-start,
                    count,
                    count / ((finish - start) / 1000),
                    System.lineSeparator());

        }
//        catch (CsvValidationException ex){
//            logger.error("CSV error '{}', exception '{}'.", inputFile == null ? "stdin" : inputFile, ex.toString());
//            if (errorStop)
//                return ExitCodes.CSV_ERROR;
//        }
        catch(Exception ex) {
            logger.error("Processing error '{}', exception '{}'.", inputFile == null ? "stdin" : inputFile, ex.toString());
            if (errorStop)
                return ExitCodes.PROCESSING_ERROR;
        }
        return ExitCodes.SUCCESS;
    }

    @Override
    public Integer call() {
        try {
            //  check parameter setting
            if (!stdIn)
                if ((inputFiles==null) || (inputFiles.length==0)){
                    logger.error("Missing parameters 'INPUT' or parameter '-s'.");
                    return ExitCodes.PARAMETR_ERROR;
                }

            // define setup
            Setup setup = Setup.getInstance(config);
            setup.setBatch(batch);

            // access to CQL engine
            CqlAccess access=new CqlAccess(setup);
            int exitCode;

            //  main processing
            if (stdIn) {
                if ((exitCode = callCore(access, null)) != ExitCodes.SUCCESS)
                    return exitCode;
            }
            else
                for (String inputFile : inputFiles)
                    if ((exitCode = callCore(access, inputFile)) != ExitCodes.SUCCESS)
                        return exitCode;
        }
        catch(IOException ex) {
            logger.error("Config error '{}'",ex.toString());
            return ExitCodes.CONFIG_ERROR;
        }
        catch(Exception ex) {
            logger.error("General error '{}'",ex.toString());
            return ExitCodes.GENERAL_ERROR;
        }
        return ExitCodes.SUCCESS;
    }

    public static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

}