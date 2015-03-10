import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by mjohnson on 2/24/15.
 */
public class PigUnitHelloWorld {
    @Test
    public void testTop2Queries() throws org.apache.pig.tools.parameters.ParseException, IOException {
        String[] args = {
                "n=2",
        };

        PigTest test = null;

        test = new PigTest("top_queries.pig", args);


        String[] input2 = {
                "yahoo",
                "yahoo",
                "yahoo",
                "twitter",
                "facebook",
                "facebook",
                "linkedin",
        };


        String[] output = {
                "(yahoo,3)",
                "(facebook,2)",
        };

        test.assertOutput("data", input2, "queries_limit", output);

    }
}
