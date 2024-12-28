package org.george0st;


import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.opencsv.exceptions.CsvValidationException;
import org.george0st.helper.ReadableTime;
import org.george0st.helper.Setup;
import org.george0st.processor.CsvCqlWrite;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;


@Command(name = "example", mixinStandardHelpOptions = true, version = "FfCsv2Cql 1.2", description = "asdasda")
public class Main implements Runnable {

    @Option(names = { "-c", "--config" },
            description = "Config file (default is 'connection.json').",
            defaultValue = "connection-private.json", paramLabel = "")
    private String config;

    @Option(names = { "-b", "--bulk" },
            description = "Bulk size (default is 200).", defaultValue = "200")
    private long bulk;

    @Parameters(arity = "1..*", paramLabel = "INPUT", description = "Input file(s) for processing.")
    private String[] inputFiles;

    @Override
    public void run() {

        try {
            Setup setup = Setup.getInstance(config);
            setup.setBulk(bulk);

            CqlAccess access=new CqlAccess(setup);
            long finish, start;

            for (String inputFile : inputFiles) {
                start = System.currentTimeMillis();
                CsvCqlWrite write = new CsvCqlWrite(access);
                write.execute(inputFile);
                finish = System.currentTimeMillis();
                System.out.println("File '" + inputFile + "': "+ ReadableTime.fromMillisec(finish - start) +
                        "(" + (finish-start) +" ms)");

            }
        }
        catch(Exception ex) {
            System.out.println(ex.toString());
        }
    }

    public static void main(String[] args) throws CsvValidationException, IOException {

        // https://github.com/timtiemens/javacommandlineparser
        // https://github.com/remkop/picocli
        // https://jcommander.org/

        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

}