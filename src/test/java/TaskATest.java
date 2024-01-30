import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TaskATest {

    @Test
    public void debug() throws Exception {
        String[] input = new String[2];

        /*
        1. put the data.txt into a folder in your PC.
        2. add the path for the following two files.
            Windows: update the path like "file:///C:/Users/.../projectDirectory/data.txt"
            Mac or Linux: update the path like "file:///Users/.../projectDirectory/data.txt"
        */

        //relative path so make sure that the code is running in the correct directory
        input[0] = "/home/taya/CS4433_Project1/src/main/data/pages.csv";
        input[1] = "/home/taya/CS4433_Project1/src/output";

        TaskA taskA = new TaskA();
        boolean result = taskA.debug(input);

        assertTrue("The Hadoop job did not complete successfully", result);
    }

}