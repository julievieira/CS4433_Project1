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

        input[0] = "file:///HostPath/project1_data/pages.csv";
        input[1] = "file:///Users/Kseniia/IdeaProjects/Project1/output";

        TaskA taskA = new TaskA();
        boolean result = taskA.debug(input);

        assertTrue("The Hadoop job did not complete successfully", result);
    }

}