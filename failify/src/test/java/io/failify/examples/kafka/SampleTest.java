package io.failify.examples.kafka;


import io.failify.FailifyRunner;
import io.failify.exceptions.RuntimeEngineException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class SampleTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleTest.class);

    protected static FailifyRunner runner;
    protected static final int NUM_OF_NODES = 3;

    @BeforeClass
    public static void before() throws RuntimeEngineException, TimeoutException {
        runner = FailifyHelper.getDeployment(NUM_OF_NODES).start();
        FailifyHelper.waitForCluster(runner);
    }

    @AfterClass
    public static void after() {
        if (runner != null) {
            runner.stop();
        }
    }


    @Test
    public void sampleTest() throws RuntimeEngineException {

    }
}
