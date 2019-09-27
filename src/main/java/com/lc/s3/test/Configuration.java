package com.lc.s3.test;

import com.amazonaws.regions.Regions;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class Configuration {
  @Option(name = "-numClients", usage = "Number of threads. Default is 1")
  private static int numClients = 1;

  @Option(name = "-noOfPrefixes", usage = "Number of S3 Prefixes. 0 means everything will be " +
          "written at the root level")
  private static boolean noOfPrefixes = false;

  @Option(name = "-bmDuration", usage = "For how long the bench mark should run. Time in ms")
  static private long benchmarkDuration = 20000;

  @Option(name = "-clientId", usage = "Id of this application.")
  static private int clientId = 0;

  @Option(name = "-skipPrompt", usage = "Do not ask for (Y/N) before starting the bench mark")
  static private boolean skipPrompt = false;

  @Option(name = "-bucketPrefix", usage = "Bucket prefix")
  static private String bucketPrefix = "hopsfs-s3-bm";

  @Option(name = "-region", usage = "S3 Region")
  static private String regionStr = "eu-north-1";
  static private Regions region = Regions.fromName(regionStr);

  @Option(name = "-numBuckets", usage = "Number of buckets")
  static private int numBuckets = 1;

  @Option(name = "-bucketDeletionThreads", usage = "Number of threads for deleting a bucket")
  static private int bucketDeletionThreads = 1;

  @Option(name = "-maxUploadThreads", usage = "Max upload threads")
  static private int maxUploadThreads = 2;

  @Option(name = "-threadlTTL", usage = "Thread TTL")
  static private int threadlTTL = 60;

  @Option(name = "-multiPartSize", usage = "Multipart size. >= 5MB (5 * 1024 * 1024)")
  static private int multiPartSize = 5 * 1024 * 1024;

  @Option(name = "-multiPartThreshold", usage = "Multipart threashold size. >= 5MB (5 * 1024 * " +
          "1024)")
  static private int multiPartThreshold = 5 * 1024 * 1024;

  @Option(name = "-tmpFolder", usage = "Folder where temp files will be created ")
  static private String tmpFolder = "/tmp";

  @Option(name = "-objSize", usage = "Size of the objects in bytes uploaded to the cloud ")
  static private int objSize = 1024 * 1024;

  @Option(name = "-deleteExistingData", usage = "Delete existing buckets")
  static private boolean deleteExistingData = true;

  @Option(name = "-help", usage = "Print usages")
  private boolean help = false;

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

  public boolean isNoOfPrefixes() {
    return noOfPrefixes;
  }

  public long getBenchmarkDuration() {
    return benchmarkDuration;
  }

  public int getClientId() {
    return clientId;
  }

  public boolean isSkipPrompt() {
    return skipPrompt;
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

  public boolean isDeleteExistingData(){
    return deleteExistingData;
  }
}
