import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.opencsv.CSVWriter;
import org.george0st.helper.RndGenerator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class PrepareTests {
    private RndGenerator rnd=new RndGenerator();
    private static String testOutput="./test_output";
    private static String testInput="./test_input";

    public PrepareTests() throws InterruptedException {
    }

    protected File getRandomFile(){
        return new File(String.format("%s/CsvToCql_%s.csv.tmp",testOutput, rnd.getStringSequence(10)));
    }

    protected void createCqlSchema(String table, String[] columns){




    }

    protected File generateRndCSVFile(int csvItems, boolean sequenceID) throws IOException {
        // generate random file name
        File randomFile=getRandomFile();
        int randomIDRange = csvItems * csvItems;

        // generate random content
        try (FileWriter writer =new FileWriter(randomFile,false)){
            try (CSVWriter csvWriter = new CSVWriter(writer)){
//                CSVWriter writer = new CSVWriter(outputFile, ';',
//                        CSVWriter.NO_QUOTE_CHARACTER,
//                        CSVWriter.DEFAULT_ESCAPE_CHARACTER,
//                        CSVWriter.DEFAULT_LINE_END);

                // write header
                csvWriter.writeNext(new String[]{"colid", "cola", "colb", "colc"});

                // write content
                for (int i=0;i<csvItems;i++) {
                    csvWriter.writeNext(new String[]{sequenceID ? Integer.toString(i) : Integer.toString(rnd.getInt(randomIDRange)),
                            rnd.getStringSequence(10),
                            rnd.getStringSequence(10),
                            rnd.getStringSequence(10)});
                }
            }
        }
        return randomFile;
    }

}
