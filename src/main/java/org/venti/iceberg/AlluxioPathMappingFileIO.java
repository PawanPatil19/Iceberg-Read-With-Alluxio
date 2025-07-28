package org.venti.iceberg;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.io.*;
import org.apache.iceberg.util.SerializableSupplier;

import java.util.Map;
import java.util.function.Function;


public class AlluxioPathMappingFileIO implements FileIO, HadoopConfigurable, SupportsPrefixOperations {

    private HadoopFileIO fileIO;
    private String alluxioBasePath;
    private String[] gcsBasePaths;
    private boolean readThroughAlluxio = true;

    public AlluxioPathMappingFileIO(Configuration conf) {
        this(new SerializableConfiguration(conf)::get);
    }

    public AlluxioPathMappingFileIO(SerializableSupplier<Configuration> conf) {
        Configuration config = conf.get();
        this.alluxioBasePath = config.get("alluxio.baseuri", "");
        this.gcsBasePaths = config.get("gcs.baseuri", "").split(",");
        this.readThroughAlluxio = config.getBoolean("read.through.alluxio", true);
        this.fileIO = new HadoopFileIO(config);
    }

    public AlluxioPathMappingFileIO() {
    }

    @Override
    public void initialize(Map<String, String> properties) {
        // Extract our custom properties
        this.alluxioBasePath = properties.getOrDefault("alluxio.baseuri", "");
        this.gcsBasePaths = properties.getOrDefault("gcs.baseuri", "").split(",");
        this.readThroughAlluxio = Boolean.parseBoolean(
                properties.getOrDefault("read.through.alluxio", "true"));

        // Validate configuration
        if (StringUtils.isBlank(alluxioBasePath) || gcsBasePaths.length == 0 ||
                StringUtils.isBlank(gcsBasePaths[0])) {
            throw new IllegalArgumentException(
                    "Both 'alluxio.baseuri' and 'gcs.baseuri' properties must be specified");
        }

        // Initialize delegate FileIO
        fileIO = new HadoopFileIO();
        fileIO.initialize(properties);
    }

    @Override
    public Map<String, String> properties() {
        return fileIO.properties();
    }

    @Override
    public void serializeConfWith(Function<Configuration, SerializableSupplier<Configuration>> function) {
        this.fileIO.serializeConfWith(function);
    }

    @Override
    public void setConf(Configuration conf) {
        this.alluxioBasePath = conf.get("alluxio.baseuri", "");
        this.gcsBasePaths = conf.get("gcs.baseuri", "").split(",");
        this.readThroughAlluxio = conf.getBoolean("read.through.alluxio", true);
        this.fileIO = new HadoopFileIO(conf);
    }

    @Override
    public Configuration getConf() {
        return fileIO.conf();
    }

    @Override
    public Iterable<FileInfo> listPrefix(String path) {
        String updatedPath = getReadPath(path);
        return fileIO.listPrefix(updatedPath);
    }

    @Override
    public void deletePrefix(String path) {
        // Delete operations should target original GCS path
        fileIO.deletePrefix(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
        // Write operations always go to original GCS path
        return fileIO.newOutputFile(path);
    }

    @Override
    public void deleteFile(String location) {
        // Delete operations should target original GCS path
        fileIO.deleteFile(location);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        String readPath = getReadPath(path);
        InputFile inputFile = fileIO.newInputFile(readPath, length);

        // If path was modified, update the location field to preserve original path
        if (!readPath.equals(path)) {
            try {
                FieldUtils.writeField(inputFile, "location", path, true);
            } catch (IllegalAccessException e) {

                throw new RuntimeException("Failed to update InputFile location", e);
            }
        }

        return inputFile;
    }

    @Override
    public InputFile newInputFile(String path) {
        String readPath = getReadPath(path);
        InputFile inputFile = fileIO.newInputFile(readPath);

        // If path was modified, update the location field to preserve original path
        if (!readPath.equals(path)) {
            try {
                FieldUtils.writeField(inputFile, "location", path, true);

            } catch (IllegalAccessException e) {

                throw new RuntimeException("Failed to update InputFile location", e);
            }
        }

        return inputFile;
    }

    @Override
    public void close() {
        if (fileIO != null) {
            fileIO.close();
        }
    }

    /**
     * Determines the actual path to use for read operations.
     * Maps GCS paths to Alluxio paths if read-through-alluxio is enabled.
     */
    private String getReadPath(final String originalPath) {
        // If read-through-alluxio is disabled, use original path
        if (!readThroughAlluxio) {
            return originalPath;
        }

        // If alluxio base path is not configured, use original path
        if (StringUtils.isBlank(alluxioBasePath)) {
            return originalPath;
        }

        // If path already starts with alluxio base path, no change needed
        if (originalPath.startsWith(alluxioBasePath)) {
            return originalPath;
        }

        // Check if path matches any of the configured GCS base paths
        for (String gcsBasePath : gcsBasePaths) {
            if (StringUtils.isNotBlank(gcsBasePath) && originalPath.startsWith(gcsBasePath)) {
                String mappedPath = originalPath.replace(gcsBasePath, alluxioBasePath);
                return mappedPath;
            }
        }

        // If no mapping found, log warning and use original path

        return originalPath;
    }
}