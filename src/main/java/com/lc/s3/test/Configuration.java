package com.lc.s3.test;

import com.amazonaws.regions.Regions;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class Configuration {

  private final int numClientsDefault = 5;
  @Option(name = "-numClients", usage = "Number of threads. Default: " + numClientsDefault)
  private int numClients = numClientsDefault;

  private final boolean usePrefixesDefault = false;
  @Option(name = "-usePrefixes", usage = "if false then all objects are written to the bucket " +
          "root. Default: " + usePrefixesDefault)
  private boolean usePrefixes = usePrefixesDefault;

  private final long prefixSizeDefault = Long.MAX_VALUE;
  @Option(name = "-prefixSize", usage = "Max number of element in each prefix. Default: " + prefixSizeDefault)
  private long prefixSize = prefixSizeDefault;

  private final long benchmarkDurationDefault = 10000;
  @Option(name = "-bmDuration", usage = "For how long the bench mark should run. Time in ms. " +
          "Default: " + benchmarkDurationDefault)
  private long benchmarkDuration = benchmarkDurationDefault;

  private final int clientIdDefault = 0;
  @Option(name = "-clientId", usage = "Id of this application. Default: " + clientIdDefault)
  private int clientId = clientIdDefault;

  private final String bucketPrefixDefault = "hopsfs-s3-bm";
  @Option(name = "-bucketPrefix", usage = "Bucket prefix. Default: " + bucketPrefixDefault)
  private String bucketPrefix = bucketPrefixDefault;

  private final String regionStrDefault = "eu-west-1";
  @Option(name = "-region", usage = "S3 Region. Default:" + regionStrDefault)
  private String regionStr = regionStrDefault;
  private Regions region = Regions.fromName(regionStr);

  private final int numBucketsDefault = 1;
  @Option(name = "-numBuckets", usage = "Number of buckets. Default: " + numBucketsDefault)
  private int numBuckets = numBucketsDefault;

  private final int bucketDeletionThreadsDefault = 50;
  @Option(name = "-bucketDeletionThreads", usage = "Number of threads for deleting a bucket. " +
          "Default: " + bucketDeletionThreadsDefault)
  private int bucketDeletionThreads = bucketDeletionThreadsDefault;

  private final int maxUploadThreadsDefault = 20;
  @Option(name = "-maxUploadThreads", usage = "Max upload threads. Default: " + maxUploadThreadsDefault)
  private int maxUploadThreads = maxUploadThreadsDefault;

  private final int threadlTTLDefault = 60;
  @Option(name = "-threadlTTL", usage = "Thread TTL in Sec. Default: " + threadlTTLDefault)
  private int threadlTTL = threadlTTLDefault;

  private final int multiPartSizeDefault = 5 * 1024 * 1024;
  @Option(name = "-multiPartSize", usage = "Multipart size. >= 5MB (5 * 1024 * 1024). Default: " + multiPartSizeDefault)
  private int multiPartSize = multiPartSizeDefault;

  private final int multiPartThresholdDefault = 5 * 1024 * 1024;
  @Option(name = "-multiPartThreshold", usage = "Multipart threashold size. >= 5MB (5 * 1024 * " +
          "1024). Default: " + multiPartThresholdDefault)
  private int multiPartThreshold = multiPartThresholdDefault;

  private final String tmpFolderDefault = "/tmp";
  @Option(name = "-tmpFolder", usage = "Folder where temp files will be created. Default: " + tmpFolderDefault)
  private String tmpFolder = tmpFolderDefault;

  private final int objSizeDefault = 1024;
  @Option(name = "-objSize", usage = "Size of the objects in bytes uploaded to the cloud. " +
          "Default: " + objSizeDefault)
  private int objSize = objSizeDefault;

  private final boolean deleteExistingDataDefault = false;
  @Option(name = "-deleteExistingData", usage = "Delete existing buckets. Default: " + deleteExistingDataDefault)
  private boolean deleteExistingData = deleteExistingDataDefault;

  private final boolean saveNSToDiskDefault = false;
  @Option(name = "-saveNSToDisk", usage =
          "Save and load namespace from disk. Default: " + saveNSToDiskDefault)
  private boolean saveNSToDisk = saveNSToDiskDefault;

  private final String diskNSFileDefault = "/tmp/namespace.bin";
  @Option(name = "-diskNSFile", usage = "File path to save namespace. Default: " + diskNSFileDefault)
  private String diskNSFile = diskNSFileDefault;

  @Option(name = "-help", usage = "Print usages")
  private boolean help = false;

  private final boolean testPutDefault = true;
  @Option(name = "-testPut", usage = "Run put test Default: " + testPutDefault)
  private boolean testPut = testPutDefault;

  private final boolean testGetDefault = false;
  @Option(name = "-testGet", usage = "Run get test. Default: " + testGetDefault)
  private boolean testGet = testGetDefault;

  private final boolean testListDefault = false;
  @Option(name = "-testList", usage = "Run list test. Default: " + testListDefault)
  private boolean testList = testListDefault;

  private final boolean testObjExistsDefault = false;
  @Option(name = "-testObjExists", usage = "Run object exists test. Default: " + testObjExistsDefault)
  private boolean testObjExists = testObjExistsDefault;

  private final boolean testDeleteDefault = false;
  @Option(name = "-testDelete", usage = "Run delete test. Default: " + testDeleteDefault)
  private boolean testDelete = testDeleteDefault;

  private final boolean testMetadataDefault = false;
  @Option(name = "-testGetMetaData", usage = "Run get obj metadata test. Default: " + testMetadataDefault)
  private boolean testMetadata = testMetadataDefault;

  private final boolean disableConnectorSharingDefault = false;
  @Option(name = "-disableConnectorSharing", usage = "Disable sharing S3 connector with the threads. Default: " + disableConnectorSharingDefault)
  private boolean disableConnectorSharing = disableConnectorSharingDefault;

  private final boolean disableTransferManagerDefault = false;
  @Option(name = "-disableS3TransferManager", usage =
          "Disable S3 Transfer manager. Default: " + disableTransferManagerDefault)
  private boolean disableS3TransferManager = disableTransferManagerDefault;

  private final boolean startPromptDefault = false;
  @Option(name = "-startPrompt", usage =
          "Enable disable Y/N prompt. Default: " + startPromptDefault)
  private boolean startPrompt = startPromptDefault;

  private final String resultFileDefault = "/tmp/results.txt";
  @Option(name = "-resultFile", usage =
          "Results file. Default: " + resultFileDefault)
  private String resultFile = resultFileDefault;

  private final int workersStartBatchSizeDefault = 10;
  @Option(name = "-workersStartBatchSize", usage =
          "Start workers in batches. Default: " + workersStartBatchSizeDefault)
  private int workersStartBatchSize = workersStartBatchSizeDefault;

  private final int workersBatchStartDelayDefault = 1000;
  @Option(name = "-workersBatchStartDelay", usage =
          "Delay between starting worker batches. Default: " + workersBatchStartDelayDefault)
  private int workersBatchStartDelay = workersBatchStartDelayDefault;

  public int getMaxUploadThreads() {
    return maxUploadThreads;
  }

  public int getThreadlTTL() {
    return threadlTTL;
  }

  public int getMultiPartSize() {
    return multiPartSize;
  }

  public int getMultiPartThreshold() {
    return multiPartThreshold;
  }

  public String getBucketPrefix() {
    return bucketPrefix;
  }

  public Regions getRegion() {
    return region;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public int getBucketDeletionThreads() {
    return bucketDeletionThreads;
  }

  public long getBenchmarkDuration() {
    return benchmarkDuration;
  }

  public int getClientId() {
    return clientId;
  }

  public void parseArgs(String[] args) {
    CmdLineParser parser = new CmdLineParser(this);
    parser.setUsageWidth(80);
    try {
      // parse the arguments.
      parser.parseArgument(args);
      region = Regions.fromName(regionStr);
    } catch (Exception e) {
      showHelp(parser, true);
    }

    if (help) {
      showHelp(parser, true);
    }
  }

  private void showHelp(CmdLineParser parser, boolean kill) {
    parser.printUsage(System.err);
    if (kill) {
      System.exit(0);
    }
  }

  public int getNumClients() {
    return numClients;
  }

  public String getTmpFolder() {
    return tmpFolder;
  }

  public int getObjSize() {
    return objSize;
  }

  public boolean isDeleteExistingData() {
    return deleteExistingData;
  }

  public boolean isSaveNSToDisk() {
    return saveNSToDisk;
  }

  public String getDiskNSFile() {
    return diskNSFile;
  }

  public long getPrefixSize() {
    return prefixSize;
  }

  public boolean isTestPut() {
    return testPut;
  }

  public boolean isTestGet() {
    return testGet;
  }

  public boolean isTestList() {
    return testList;
  }

  public boolean isTestObjExists() {
    return testObjExists;
  }

  public boolean isTestMetadata() {
    return testMetadata;
  }

  public boolean isTestDelete() {
    return testDelete;
  }

  public boolean isUsePrefixes() {
    return usePrefixes;
  }

  public boolean isDisableConnectorSharing() {
    return disableConnectorSharing;
  }

  public boolean isDisableS3TransferManager() {
    return disableS3TransferManager;
  }

  public boolean isStartPrompt() {
    return startPrompt;
  }

  public String getResultFile() {
    return resultFile;
  }

  public int getWorkersBatchStartDelay() {
    return workersBatchStartDelay;
  }

  public int getWorkersStartBatchSize() {
    return workersStartBatchSize;
  }

  public String getParams() {
    StringBuffer sb = new StringBuffer("");

    sb.append("______________________________________________").append("\n");
    sb.append("numClients : ").append(numClients).append("\n");
    sb.append("usePrefixes : ").append(usePrefixes).append("\n");
    sb.append("prefixSize : ").append(prefixSize).append("\n");
    sb.append("benchmarkDuration : ").append(benchmarkDuration).append("\n");
    sb.append("clientId : ").append(clientId).append("\n");
    sb.append("bucketPrefix : ").append(bucketPrefix).append("\n");
    sb.append("regionStr : ").append(regionStr).append("\n");
    sb.append("numBuckets : ").append(numBuckets).append("\n");
    sb.append("bucketDeletionThreads : ").append(bucketDeletionThreads).append("\n");
    sb.append("maxUploadThreads : ").append(maxUploadThreads).append("\n");
    sb.append("threadlTTL : ").append(threadlTTL).append("\n");
    sb.append("multiPartSize : ").append(multiPartSize).append("\n");
    sb.append("multiPartThreshold : ").append(multiPartThreshold).append("\n");
    sb.append("tmpFolder : ").append(tmpFolder).append("\n");
    sb.append("objSize : ").append(objSize).append("\n");
    sb.append("deleteExistingData : ").append(deleteExistingData).append("\n");
    sb.append("saveNSToDisk : ").append(saveNSToDisk).append("\n");
    sb.append("diskNSFile : ").append(diskNSFile).append("\n");
    sb.append("disableConnectorSharing : ").append(disableConnectorSharing).append("\n");
    sb.append("disableS3TransferManager : ").append(disableS3TransferManager).append("\n");
    sb.append("workersStartBatchSize : ").append(workersStartBatchSize).append("\n");
    sb.append("workersBatchStartDelay :  ").append(workersBatchStartDelay).append("\n");

    return sb.toString();

  }
}
