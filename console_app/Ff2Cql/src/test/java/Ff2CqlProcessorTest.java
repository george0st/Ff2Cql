import org.george0st.CqlCreateSchema;
import org.george0st.helper.ReadableValue;
import org.george0st.processor.CsvCqlValidate;
import org.george0st.processor.CsvCqlWrite;
import org.george0st.helper.Setup;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.File;



class Ff2CqlProcessorTest {

    private final static String testOutput="./test_output";
    private final static String testInput="./test_input";
    private String testSetupFile;
    private CqlCreateSchema schema;

    Ff2CqlProcessorTest() throws InterruptedException, IOException {
        // select valid config/json file
        testSetupFile = Setup.getSetupFile(testInput, new String[]{"test-connection-private.json", "test-connection.json"});

        // create outputPath
        File outputPath=new File(testOutput);
        if (!outputPath.exists())
            outputPath.mkdir();

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

    void coreTest(File randomFile, boolean validateAlso) throws IOException, InterruptedException {
        long finish, start, count;

        // write
        start = System.currentTimeMillis();
        count = (new CsvCqlWrite(Setup.getInstance(testSetupFile), false)).execute(randomFile==null ? null : randomFile.getPath());
        finish = System.currentTimeMillis();
        System.out.println("WRITE; Items: " + ReadableValue.fromNumber(count) + "; " +
                String.format("Perf: %.1f [calls/sec]; ", count / ((finish-start) / 1000.0)) +
                "Duration: " + ReadableValue.fromMillisecond(finish - start));

        if ((validateAlso) && (randomFile!=null)) {
            // delay (before read for synch on CQL side)
            Thread.sleep(3000);

            // validate (read value from CSV and from CQL and compare content)
            start = System.currentTimeMillis();
            count = (new CsvCqlValidate(Setup.getInstance(testSetupFile), schema.getPrimaryKeys())).execute(randomFile.getPath());
            finish = System.currentTimeMillis();
            System.out.println("VALIDATE; Items: " + ReadableValue.fromNumber(count) + "; " +
                    String.format("Perf: %.1f [calls/sec]; ", count / ((finish-start) / 1000.0)) +
                    "Duration: " + ReadableValue.fromMillisecond(finish - start));
        }
    }

    @Test
    @DisplayName("Sequence WR, 1. 100 items in CSV")
    void csvWRSequence100() throws Exception {
        File randomFile=schema.generateRndCSVFile(100, true);
        coreTest(randomFile, true);
    }

    @Test
    @DisplayName("Sequence WR, 2. 1K items in CSV")
    void csvWRSequence1K() throws IOException, InterruptedException {
        File randomFile=schema.generateRndCSVFile(1_000, true);
        coreTest(randomFile, true);
    }

    @Test
    @DisplayName("Sequence W, 3. 10K items in CSV")
    void csvWSequence10K() throws IOException, InterruptedException {
        File randomFile=schema.generateRndCSVFile(10_000, true);
        coreTest(randomFile, false);
    }

    @Test
    @DisplayName("Sequence W, 4. 100K items in CSV")
    void csvWSequence100K() throws IOException, InterruptedException {
        File randomFile=schema.generateRndCSVFile(100_000, true);
        coreTest(randomFile, false);
    }

    @Test
    @Disabled
    @DisplayName("Sequence W, 5. 1M items in CSV")
    void csvWSequence1M() throws IOException, InterruptedException {
        File randomFile=schema.generateRndCSVFile(1_000_000, true);
        coreTest(randomFile, false);
    }

    @Test
    @DisplayName("Random WR, 1. 100 items in CSV")
    void csvWRRandom100() throws IOException, InterruptedException {
        File randomFile=schema.generateRndCSVFile(100, false);
        coreTest(randomFile, true);
    }

    @Test
    @DisplayName("Random WR, 2. 1K items in CSV")
    void csvWRRandom1K() throws IOException, InterruptedException {
        File randomFile=schema.generateRndCSVFile(1_000, false);
        coreTest(randomFile, true);
    }

    @Test
    @DisplayName("Random W, 3. 10K items in CSV")
    void csvWRandom10K() throws IOException, InterruptedException {
        File randomFile=schema.generateRndCSVFile(10_000, false);
        coreTest(randomFile, false);
    }

    @Test
    @DisplayName("Random W, 4. 100K items in CSV")
    void csvWRandom100K() throws IOException, InterruptedException {
        File randomFile=schema.generateRndCSVFile(100_000, false);
        coreTest(randomFile, false);
    }

    @Test
    @Disabled
    @DisplayName("Random W, 5. 1M items in CSV")
    void csvWRandom1M() throws IOException, InterruptedException {
        File randomFile=schema.generateRndCSVFile(1_000_000, false);
        coreTest(randomFile, false);
    }

    @Test
    @DisplayName("Only header WR, O items in CSV")
    void csvWROnlyCSVHeader() throws IOException, InterruptedException {
        File randomFile=schema.generateRndCSVFile(0, true);
        coreTest(randomFile, true);
    }

    @Test
    @DisplayName("Empty file WR, O items in CSV")
    void csvWREmptyCSV() throws IOException, InterruptedException {
        File randomFile=schema.generateRndCSVFile(-1, true);
        coreTest(randomFile, true);
    }

}