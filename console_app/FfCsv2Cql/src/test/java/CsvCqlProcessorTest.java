import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.george0st.processor.CsvCqlRead;
import org.george0st.processor.CsvCqlWrite;
import org.george0st.helper.RndGenerator;
import org.george0st.helper.Setup;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.File;
import java.io.FileWriter;

//  https://www.vogella.com/tutorials/JUnit/article.html#junitsetup
class CsvCqlProcessorTest {

    private RndGenerator rnd=new RndGenerator();
    private static String testOutput="./test_output";
    private static String testInput="./test_input";

    CsvCqlProcessorTest() throws InterruptedException {

    }


    private File getRandomFile(){
        return new File(String.format("%s/CsvToCql_%s.csv.tmp",testOutput, rnd.getStringSequence(10)));
    }

    private static void cleanUp(){
        //  remove all random files from testOutput directory
        File[] contents = new File(testOutput).listFiles();
        if (contents != null)
            for (File f : contents)
                f.delete();
    }

    @BeforeAll
    public static void setUp() {
        cleanUp();
    }

    private File generateRndCSVFile(int csvItems, boolean sequenceID) throws IOException {
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

    @Test
    @DisplayName("Sequence, 1. 100 items in CSV")
    void csvSequence100() throws IOException, CsvValidationException, InterruptedException {
        File randomFile = generateRndCSVFile(100, true);

        // write to CQL
        String testSetupFile = Setup.getSetupFile(new String[]{
                String.format("%s/test-connection-private.json", testInput),
                String.format("%s/test-connection.json", testInput)});
        CsvCqlWrite write=new CsvCqlWrite(Setup.getInstance(testSetupFile));
        write.execute(randomFile.getPath());

        // delay, before read
        Thread.sleep(3000);

        // validation, read from CQL
        CsvCqlRead read = new CsvCqlRead(Setup.getInstance(testSetupFile), new String[]{"colid", "cola"});
        read.execute(randomFile.getPath());
    }

    //@RepeatedTest(3)
    @Test
    @DisplayName("Sequence, 2. 1K items in CSV")
    void csvSequence1K() throws IOException, CsvValidationException {
        File randomFile = generateRndCSVFile(1000, true);

        // write to CQL
        String testSetupFile = Setup.getSetupFile(new String[]{
                String.format("%s/test-connection-private.json", testInput),
                String.format("%s/test-connection.json", testInput)});
        CsvCqlWrite processor=new CsvCqlWrite(Setup.getInstance(testSetupFile));
        processor.execute(randomFile.getPath());

        // validation, read from CQL
    }

    @Test
    @DisplayName("Sequence, 3. 10K items in CSV")
    void csvSequence10K() throws IOException, CsvValidationException {
        File randomFile=generateRndCSVFile(10000, true);

        // write to CQL
        String testSetupFile = Setup.getSetupFile(new String[]{
                String.format("%s/test-connection-private.json", testInput),
                String.format("%s/test-connection.json", testInput)});
        CsvCqlWrite processor=new CsvCqlWrite(Setup.getInstance(testSetupFile));
        processor.execute(randomFile.getPath());

        // validation, read from CQL
    }

    @Test
    @DisplayName("Random, 1. 100 items in CSV")
    void csvRandom100() throws IOException, CsvValidationException {
        File randomFile = generateRndCSVFile(100, false);

        // write to CQL
        String testSetupFile = Setup.getSetupFile(new String[]{
                String.format("%s/test-connection-private.json", testInput),
                String.format("%s/test-connection.json", testInput)});
        CsvCqlWrite processor=new CsvCqlWrite(Setup.getInstance(testSetupFile));
        processor.execute(randomFile.getPath());

        // validation, read from CQL
    }

    //@RepeatedTest(10)
    @Test
    @DisplayName("Random, 2. 1K items in CSV")
    void csvRandom1K() throws IOException, CsvValidationException {
        File randomFile=generateRndCSVFile(1000, false);

        // write to CQL
        String testSetupFile = Setup.getSetupFile(new String[]{
                String.format("%s/test-connection-private.json", testInput),
                String.format("%s/test-connection.json", testInput)});
        CsvCqlWrite processor=new CsvCqlWrite(Setup.getInstance(testSetupFile));
        processor.execute(randomFile.getPath());

        // validation, read from CQL
    }

    @Test
    @DisplayName("Random, 3. 10K items in CSV")
    void csvRandom10K() throws IOException, CsvValidationException {
        File randomFile=generateRndCSVFile(10000, false);

        // write to CQL
        String testSetupFile = Setup.getSetupFile(new String[]{
                String.format("%s/test-connection-private.json", testInput),
                String.format("%s/test-connection.json", testInput)});
        CsvCqlWrite processor=new CsvCqlWrite(Setup.getInstance(testSetupFile));
        processor.execute(randomFile.getPath());

        // validation, read from CQL
    }

}