import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.opencsv.CSVWriter;
import org.george0st.RndGenerator;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.io.File;
import java.io.FileWriter;

//  https://www.vogella.com/tutorials/JUnit/article.html#junitsetup
class CsvCqlProcessorTest {

    private RndGenerator rnd=new RndGenerator();
    private static String testOutput="./test_output";

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

    private void generateRandomCSV(int csvItems, boolean sequenceID) throws IOException {
        // generate random file name
        File file=getRandomFile();
        int randomIDRange = csvItems * csvItems;

        // generate random content
        try (FileWriter writer =new FileWriter(file,false)){
            try (CSVWriter csvWriter = new CSVWriter(writer)){
//                CSVWriter writer = new CSVWriter(outputfile, ';',
//                        CSVWriter.NO_QUOTE_CHARACTER,
//                        CSVWriter.DEFAULT_ESCAPE_CHARACTER,
//                        CSVWriter.DEFAULT_LINE_END);

                // write header
                csvWriter.writeNext(new String[]{"colID", "colA", "colB", "colC"});

                // write content
                for (int i=0;i<csvItems;i++) {
                    csvWriter.writeNext(new String[]{sequenceID ? Integer.toString(i) : Integer.toString(rnd.getInt(randomIDRange)),
                            rnd.getStringSequence(10),
                            rnd.getStringSequence(10),
                            rnd.getStringSequence(10)});
                }
            }
        }
    }

    @Test
    @DisplayName("Sequence, 1. 100 items in CSV")
    void csvSequence100() throws IOException {
        generateRandomCSV(100, true);

        // write to CQL

        // validation, read from CQL
    }

    //@RepeatedTest(3)
    @Test
    @DisplayName("Sequence, 2. 1K items in CSV")
    void csvSequence1K() throws IOException {
        generateRandomCSV(1000, true);

        // write to CQL

        // validation, read from CQL
    }

    @Test
    @DisplayName("Random, 1. 100 items in CSV")
    void csvRandom100() throws IOException {
        generateRandomCSV(100, false);

        // write to CQL

        // validation, read from CQL
    }

    //@RepeatedTest(10)
    @Test
    @DisplayName("Random, 2. 1K items in CSV")
    void csvRandom1K() throws IOException {
        generateRandomCSV(1000, false);

        // write to CQL

        // validation, read from CQL
    }

}