import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Path;
import java.util.Random;
import java.util.random.RandomGenerator;

//  https://www.vogella.com/tutorials/JUnit/article.html#junitsetup
class CsvCqlProcessorTest {

    String aaa;

    String getRandomFile(){
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        int targetStringLength = 5;
        Random random = new Random();

        String fileName = random.ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();

        try {
            File temp = File.createTempFile("CsvToCql_", ".csv.tmp");
            System.out.println("Temp file : " + temp.getAbsolutePath());

            temp.deleteOnExit();

            //Path tmpdir = Files.createTempDirectory(Paths.get("target"), "tmpDirPrefix");


            String absolutePath = temp.getAbsolutePath();
            String tempFilePath = absolutePath
                    .substring(0, absolutePath.lastIndexOf(File.separator));
        }catch (IOException e) {
            e.printStackTrace();
        }


        return fileName;
    }

    @BeforeEach
    void setUp() {
        aaa = new String();
    }

    @Test
    void csv1kItems(@TempDir Path tempDir) {

        //  generate data
        String aa=getRandomFile();

//        try (Writer reader = new FileWriter(generatedString)) {
//
//        }
        System.out.println(tempDir);
        assertTrue(true);
    }

    @RepeatedTest(5)
    @DisplayName("Ensure correct handling of zero")
    void testMultiplyWithZero() {
        assertTrue(true);
    }
}