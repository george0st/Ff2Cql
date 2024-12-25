import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import org.george0st.CqlAccess;
import org.george0st.CqlCreateSchema;
import org.george0st.processor.CsvCqlRead;
import org.george0st.processor.CsvCqlWrite;
import org.george0st.helper.RndGenerator;
import org.george0st.helper.Setup;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.File;
import java.io.FileWriter;

//  https://www.vogella.com/tutorials/JUnit/article.html#junitsetup
class CsvCqlProcessorTest{

    private RndGenerator rnd=new RndGenerator();
    private static String testOutput="./test_output";
    private static String testInput="./test_input";

    CsvCqlProcessorTest() throws InterruptedException {

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


    @Test
    @DisplayName("Sequence, 1. 100 items in CSV")
    void csvSequence100() throws Exception {

        // select valid config/json file
        String testSetupFile = Setup.getSetupFile(testInput, new String[]{"test-connection-private.json", "test-connection.json"});

        // create schema for testing
        CqlCreateSchema schema=new CqlCreateSchema(Setup.getInstance(testSetupFile));
        schema.Create();
        File randomFile=schema.generateRndCSVFile(100, true);

        // write
        CsvCqlWrite write=new CsvCqlWrite(Setup.getInstance(testSetupFile));
        write.execute(randomFile.getPath());

        // delay (before read)
        Thread.sleep(3000);

        // read/validate
        CsvCqlRead read = new CsvCqlRead(Setup.getInstance(testSetupFile), schema.getPrimaryKeys());
        read.execute(randomFile.getPath());
    }

    //@RepeatedTest(3)
    @Test
    @DisplayName("Sequence, 2. 1K items in CSV")
    void csvSequence1K() throws IOException, CsvValidationException {
//        File randomFile = generateRndCSVFile(1000, true);
//
//        // write to CQL
//        String testSetupFile = Setup.getSetupFile(new String[]{
//                String.format("%s/test-connection-private.json", testInput),
//                String.format("%s/test-connection.json", testInput)});
//        CsvCqlWrite processor=new CsvCqlWrite(Setup.getInstance(testSetupFile));
//        processor.execute(randomFile.getPath());
//
//        // validation, read from CQL
    }

    @Test
    @DisplayName("Sequence, 3. 10K items in CSV")
    void csvSequence10K() throws IOException, CsvValidationException {
//        File randomFile=generateRndCSVFile(10000, true);
//
//        // write to CQL
//        String testSetupFile = Setup.getSetupFile(new String[]{
//                String.format("%s/test-connection-private.json", testInput),
//                String.format("%s/test-connection.json", testInput)});
//        CsvCqlWrite processor=new CsvCqlWrite(Setup.getInstance(testSetupFile));
//        processor.execute(randomFile.getPath());
//
//        // validation, read from CQL
    }

    @Test
    @DisplayName("Random, 1. 100 items in CSV")
    void csvRandom100() throws IOException, CsvValidationException {
//        File randomFile = generateRndCSVFile(100, false);
//
//        // write to CQL
//        String testSetupFile = Setup.getSetupFile(new String[]{
//                String.format("%s/test-connection-private.json", testInput),
//                String.format("%s/test-connection.json", testInput)});
//        CsvCqlWrite processor=new CsvCqlWrite(Setup.getInstance(testSetupFile));
//        processor.execute(randomFile.getPath());
//
//        // validation, read from CQL
    }

    //@RepeatedTest(10)
    @Test
    @DisplayName("Random, 2. 1K items in CSV")
    void csvRandom1K() throws IOException, CsvValidationException {
//        File randomFile=generateRndCSVFile(1000, false);
//
//        // write to CQL
//        String testSetupFile = Setup.getSetupFile(new String[]{
//                String.format("%s/test-connection-private.json", testInput),
//                String.format("%s/test-connection.json", testInput)});
//        CsvCqlWrite processor=new CsvCqlWrite(Setup.getInstance(testSetupFile));
//        processor.execute(randomFile.getPath());
//
//        // validation, read from CQL
    }

    @Test
    @DisplayName("Random, 3. 10K items in CSV")
    void csvRandom10K() throws IOException, CsvValidationException {
//        File randomFile=generateRndCSVFile(10000, false);
//
//        // write to CQL
//        String testSetupFile = Setup.getSetupFile(new String[]{
//                String.format("%s/test-connection-private.json", testInput),
//                String.format("%s/test-connection.json", testInput)});
//        CsvCqlWrite processor=new CsvCqlWrite(Setup.getInstance(testSetupFile));
//        processor.execute(randomFile.getPath());
//
//        // validation, read from CQL
    }

}