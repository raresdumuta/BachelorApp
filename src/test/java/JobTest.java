import org.junit.Test;
import rares.Job;

import java.io.IOException;

public class JobTest {

    @Test
    public void testReadWrite() throws IOException {
        String inputPath = "src/test/resources/input.tsv";
        String output = "src/test/resources/output.csv";

        String[] args = new String[]{
                "input=" + inputPath,
                "output=" + output
        };
        Job.main(args);
    }
}
