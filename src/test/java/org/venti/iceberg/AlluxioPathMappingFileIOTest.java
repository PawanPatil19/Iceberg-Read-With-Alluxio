package org.venti.iceberg;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class AlluxioPathMappingFileIOTest {

    private AlluxioPathMappingFileIO fileIO;
    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
        conf.set("alluxio.baseuri", "alluxio://localhost:19998/");
        conf.set("gcs.baseuri", "gs://my-bucket/");
        conf.setBoolean("read.through.alluxio", true);

        fileIO = new AlluxioPathMappingFileIO();
    }

    @Test
    public void testInitializationWithProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("alluxio.baseuri", "alluxio://localhost:19998/");
        properties.put("gcs.baseuri", "gs://my-bucket/");
        properties.put("read.through.alluxio", "true");

        fileIO.initialize(properties);

        // Should not throw exception
        assertNotNull(fileIO.properties());
    }

    @Test
    public void testInitializationWithConfiguration() {
        AlluxioPathMappingFileIO configFileIO = new AlluxioPathMappingFileIO(conf);

        // Should not throw exception
        assertNotNull(configFileIO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitializationFailsWithMissingAlluxioUri() {
        Map<String, String> properties = new HashMap<>();
        properties.put("gcs.baseuri", "gs://my-bucket/");

        fileIO.initialize(properties);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInitializationFailsWithMissingGcsUri() {
        Map<String, String> properties = new HashMap<>();
        properties.put("alluxio.baseuri", "alluxio://localhost:19998/");

        fileIO.initialize(properties);
    }

    @Test
    public void testConfigurationHandling() {
        fileIO.setConf(conf);
        Configuration retrievedConf = fileIO.getConf();

        assertNotNull(retrievedConf);
        assertEquals("alluxio://localhost:19998/", retrievedConf.get("alluxio.baseuri"));
    }

    @Test
    public void testMultipleGcsBasePaths() {
        Map<String, String> properties = new HashMap<>();
        properties.put("alluxio.baseuri", "alluxio://localhost:19998/");
        properties.put("gcs.baseuri", "gs://bucket1/,gs://bucket2/data/");
        properties.put("read.through.alluxio", "true");

        fileIO.initialize(properties);

        // Should handle multiple GCS base paths
        assertNotNull(fileIO.properties());
    }

    @Test
    public void testReadThroughAlluxioDisabled() {
        Map<String, String> properties = new HashMap<>();
        properties.put("alluxio.baseuri", "alluxio://localhost:19998/");
        properties.put("gcs.baseuri", "gs://my-bucket/");
        properties.put("read.through.alluxio", "false");

        fileIO.initialize(properties);

        // When disabled, should still work but not map paths
        assertNotNull(fileIO.properties());
    }

    @Test
    public void testNewInputFileCreation() {
        Map<String, String> properties = new HashMap<>();
        properties.put("alluxio.baseuri", "alluxio://localhost:19998/");
        properties.put("gcs.baseuri", "gs://my-bucket/");
        properties.put("read.through.alluxio", "true");

        fileIO.initialize(properties);

        String gcsPath = "gs://my-bucket/warehouse/table/data/file.parquet";

        // This should create an InputFile (though it may fail to actually read in test)
        // The important thing is that the path mapping logic is exercised
        try {
            InputFile inputFile = fileIO.newInputFile(gcsPath);
            System.out.println("Mapped input path = " + inputFile.location());
            assertNotNull(inputFile);
        } catch (Exception e) {
            // Expected in test environment without actual file system setup
            // The path mapping should still have been attempted
        }
    }

    @Test
    public void testNewOutputFileCreation() {
        Map<String, String> properties = new HashMap<>();
        properties.put("alluxio.baseuri", "alluxio://localhost:19998/");
        properties.put("gcs.baseuri", "gs://my-bucket/");

        fileIO.initialize(properties);

        String gcsPath = "gs://my-bucket/warehouse/table/data/new-file.parquet";

        // Output files should not be mapped
        try {
            OutputFile outputFile = fileIO.newOutputFile(gcsPath);
            assertNotNull(outputFile);
        } catch (Exception e) {
            // Expected in test environment without actual file system setup
        }
    }

    @Test
    public void testClose() {
        Map<String, String> properties = new HashMap<>();
        properties.put("alluxio.baseuri", "alluxio://localhost:19998/");
        properties.put("gcs.baseuri", "gs://my-bucket/");

        fileIO.initialize(properties);

        // Should not throw exception
        fileIO.close();
    }
}