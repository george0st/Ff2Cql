import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.opencsv.exceptions.CsvValidationException;
import org.george0st.CqlCreateSchema;
import org.george0st.helper.ReadableTime;
import org.george0st.processor.CsvCqlValidate;
import org.george0st.processor.CsvCqlWrite;
import org.george0st.helper.RndGenerator;
import org.george0st.helper.Setup;
import org.junit.jupiter.api.*;

import javax.management.InvalidAttributeValueException;
import java.io.IOException;
import java.io.File;

//  https://www.vogella.com/tutorials/JUnit/article.html#junitsetup
class Ff2CqlProcessorTest {

    private RndGenerator rnd=new RndGenerator();
    private static String testOutput="./test_output";
    private static String testInput="./test_input";
    private String testSetupFile;
    private CqlCreateSchema schema;

    Ff2CqlProcessorTest() throws InterruptedException, CsvValidationException, IOException, InvalidAttributeValueException {
        // select valid config/json file
        testSetupFile = Setup.getSetupFile(testInput, new String[]{"test-connection-private.json", "test-connection.json"});

        // create schema for testing
        schema=new CqlCreateSchema(Setup.getInstance(testSetupFile));
        schema.Create();
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

    void coreTest(File randomFile, boolean writeAlso) throws CsvValidationException, IOException, InterruptedException, InvalidAttributeValueException {
        long finish, start;

        // write
        start = System.currentTimeMillis();
        new CsvCqlWrite(Setup.getInstance(testSetupFile)).execute(randomFile.getPath());
        finish = System.currentTimeMillis();
        System.out.println("WRITE duration: " + ReadableTime.fromMillisecond(finish - start));

        if (writeAlso) {
            // delay (before read)
            Thread.sleep(3000);

            // read/validate
            start = System.currentTimeMillis();
            new CsvCqlValidate(Setup.getInstance(testSetupFile), schema.getPrimaryKeys()).execute(randomFile.getPath());
            finish = System.currentTimeMillis();
            System.out.println("READ/VALIDATE duration: " + ReadableTime.fromMillisecond(finish - start));
        }
    }

    @Test
    @DisplayName("Sequence WR, 1. 100 items in CSV")
    void csvWRSequence100() throws Exception {
        File randomFile=schema.generateRndCSVFile(100, true);
        coreTest(randomFile, true);
    }

    //@RepeatedTest(3)
    @Test
    @DisplayName("Sequence WR, 2. 1K items in CSV")
    void csvWRSequence1K() throws IOException, CsvValidationException, InterruptedException, InvalidAttributeValueException {
        File randomFile=schema.generateRndCSVFile(1000, true);
        coreTest(randomFile, true);
    }

    @Test
    @DisplayName("Sequence W, 3. 10K items in CSV")
    void csvWSequence10K() throws IOException, CsvValidationException, InterruptedException, InvalidAttributeValueException {
        File randomFile=schema.generateRndCSVFile(10000, true);
        coreTest(randomFile, false);
    }

    @Test
    @DisplayName("Random WR, 1. 100 items in CSV")
    void csvWRRandom100() throws IOException, CsvValidationException, InterruptedException, InvalidAttributeValueException {
        File randomFile=schema.generateRndCSVFile(100, false);
        coreTest(randomFile, true);
    }

    //@RepeatedTest(10)
    @Test
    @DisplayName("Random WR, 2. 1K items in CSV")
    void csvWRRandom1K() throws IOException, CsvValidationException, InterruptedException, InvalidAttributeValueException {
        File randomFile=schema.generateRndCSVFile(1000, false);
        coreTest(randomFile, true);
    }

    @Test
    @DisplayName("Random W, 3. 10K items in CSV")
    void csvWRandom10K() throws IOException, CsvValidationException, InterruptedException, InvalidAttributeValueException {
        File randomFile=schema.generateRndCSVFile(10000, false);
        coreTest(randomFile, false);
    }
}