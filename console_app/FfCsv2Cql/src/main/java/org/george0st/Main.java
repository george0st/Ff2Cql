package org.george0st;

import org.george0st.helper.ReadableTime;
import org.george0st.helper.Setup;
import org.george0st.processor.CsvCqlWrite;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Command(name = "example", mixinStandardHelpOptions = true, version = "FfCsv2Cql 1.2", description = "Simple transfer data from NiFi FileFlow to CQL.")
public class Main implements Runnable {

    @Option(names = { "-c", "--config" },
            description = "Config file (default is 'connection.json').",
            defaultValue = "connection-private.json")
    private String config;

    @Option(names = { "-b", "--bulk" },
            description = "Bulk size (default is 200).", defaultValue = "200")
    private long bulk;

    @Parameters(arity = "1..*", paramLabel = "INPUT", description = "Input file(s) for processing.")
    private String[] inputFiles;

    @Override
    public void run() {

        try {
            // define setup
            Setup setup = Setup.getInstance(config);
            setup.setBulk(bulk);

            // general access
            CqlAccess access=new CqlAccess(setup);
            long finish, start;

            //  processing files
            for (String inputFile : inputFiles) {
                try {
                    start = System.currentTimeMillis();

                    //  write file
                    CsvCqlWrite write = new CsvCqlWrite(access);
                    write.execute(inputFile);

                    finish = System.currentTimeMillis();
                    System.out.println("File '" + inputFile + "': " + ReadableTime.fromMillisecond(finish - start) +
                            "(" + (finish - start) + " ms)");
                }
                catch(Exception ex) {
                    logger.error(String.format("Processing '%s', Exception '%s'.", inputFile, ex));
                }
            }
        }
        catch(Exception ex) {
            logger.error(String.format("General '%s'",ex));
        }
    }

    public static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
//
//        logger.debug("This is a debug message");
//        logger.info("This is an info message");
//        logger.warn("This is a warn message");
//        logger.error("This is an error message");


//        // Cesta k souboru log4j.properties
//        String log4jConfigFile = "log4j.properties";
//        // Načtení konfigurace Log4j Property
//        Configurator.configure(log4jConfigFile);
//
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

}