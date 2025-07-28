# Iceberg-Read-With-Alluxio


## Overview
`AlluxioPathMappingFileIO` is a custom `FileIO` implementation for[Apache Iceberg](https://iceberg.apache.org/) that allows seamless redirection of read operations to [Alluxio](https://www.alluxio.io/) while maintaining write/delete operations directly on Google Cloud Storage (GCS). This can be useful for performance optimization in read-heavy Iceberg workloads, where leveraging Alluxio's in-memory caching and tiered storage helps reduce latency and cloud I/O costs.
It works by intercepting file read requests and dynamically replacing the GCS base path with an Alluxio base path, ensuring reads are served from Alluxio if available. Write and delete operations are intentionally kept direct-to-GCS to maintain consistency and durability of the data lake. This design improves performance and reduces cloud I/O, all while remaining fully compatible with existing Iceberg catalog and table configurations.

## Features

- **Read-through Alluxio**: Transparently maps GCS paths to Alluxio for input operations (e.g., reading metadata or data files).
- **Write-through to GCS**: Ensures output operations (e.g., writing manifest/data files) are committed to GCS.
- **Prefix operations support**: Supports listing and deleting files with prefix operations using the correct target path.
- **Easy integration with Iceberg**: Implements Iceberg’s `FileIO`, `HadoopConfigurable`, and `SupportsPrefixOperations`.

---

## Motivation

In cloud-based data lakes using Iceberg over object stores like GCS, I/O latency for metadata operations and frequent file reads can become a bottleneck. Alluxio provides a distributed cache and file system abstraction that improves read performance. This `FileIO` implementation allows you to transparently use Alluxio for reading Iceberg metadata and data files, while ensuring data integrity by writing directly to the object store.

---

## Usage

### 1. Add the JAR to Your Classpath

Build the JAR and include it in your Spark or Iceberg application.

### 2. Configuration Properties

The following properties are required to correctly configure the `AlluxioPathMappingFileIO`. These can be supplied through either:

- Hadoop `Configuration` (e.g., `core-site.xml`, `spark-defaults.conf`), or
- Iceberg catalog properties (in `iceberg.catalog.<catalog_name>.io-impl` configurations)

| Property Key            | Required | Default | Description                                                                                     | Implementation Reference                                  |
|------------------------|----------|---------|-------------------------------------------------------------------------------------------------|-----------------------------------------------------------|
| `alluxio.baseuri`      | ✅ Yes   | `""`    | Base path prefix for Alluxio (used to construct Alluxio read paths).                            | Used in: `initialize()`, `setConf()`, `getReadPath()`     |
| `gcs.baseuri`          | ✅ Yes   | `""`    | Comma-separated list of GCS base paths to match and replace with Alluxio base path on reads.    | Used in: `initialize()`, `setConf()`, `getReadPath()`     |
| `read.through.alluxio` | ❌ No    | `true`  | Enables or disables read path remapping to Alluxio. If `false`, reads go directly to GCS.       | Used in: `initialize()`, `setConf()`, `getReadPath()`     |

---

For Spark:
```bash
--conf spark.sql.catalog.my_catalog.type=hadoop
--conf spark.hadoop.fs.alluxio.impl=alluxio.hadoop.FileSystem
--conf spark.sql.catalog.my_catalog.io-impl=org.venti.iceberg.AlluxioPathMappingFileIO
--conf spark.sql.catalog.my_catalog.alluxio.baseuri=alluxio://xxx
--conf spark.sql.catalog.my_catalog.gcs.baseuri=gs://xxx
