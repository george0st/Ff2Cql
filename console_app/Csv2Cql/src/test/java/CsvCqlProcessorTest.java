import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.CSVWriterBuilder;
import org.george0st.RndGenerator;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import java.util.random.RandomGenerator;

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
        int randomSize= csvItems*csvItems;


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
                for (int i =0;i<csvItems;i++) {

                    csvWriter.writeNext(new String[]{sequenceID ? Integer.toString(i) : Integer.toString(rnd.getNumber(randomSize)),
                            rnd.getStringSequence(10),
                            rnd.getStringSequence(10),
                            rnd.getStringSequence(10)});
                }

            }
        }
    }


    @RepeatedTest(3)
    @DisplayName("Sequence 1K items in CSV")
    void csvSequence1K() throws IOException {

        generateRandomCSV(1000, true);


        // write to CQL

        // validation, read from CQL
    }

    @RepeatedTest(10)
    @DisplayName("Random 1K items in CSV")
    void csvRandom1K() throws IOException {
        generateRandomCSV(1000, false);

        // write to CQL

        // validation, read from CQL

    }

    @Test
    void csv1kItems(@TempDir Path tempDir) {

        //  generate data
        //String aa=getRandomFile();

//        try (Writer reader = new FileWriter(generatedString)) {
//
//        }
        System.out.println(tempDir);
        assertTrue(true);
    }


}