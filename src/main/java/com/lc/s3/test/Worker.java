package com.lc.s3.test;

import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;
import sun.misc.UUDecoder;
import sun.security.krb5.Config;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class Worker implements Callable {
  final AtomicInteger successfulOps;
  final AtomicInteger failedOps;
  final SynchronizedDescriptiveStatistics latency;
  final Random rand = new Random(System.nanoTime());
  final Configuration conf;
  final CloudPersistenceProvider cloudConnector;
  final Test test;
  int counter = 0;
  long bmStartTime = 0;
  File tempFile;

  public Worker(Test test, AtomicInteger successfulOps, AtomicInteger failedOps,
                SynchronizedDescriptiveStatistics lagency, Configuration conf,
                CloudPersistenceProvider cloudConnector) throws IOException {
    this.successfulOps = successfulOps;
    this.failedOps = failedOps;
    this.latency = lagency;
    this.conf = conf;
    this.cloudConnector = cloudConnector;
    this.test = test;
    createTempFile();
  }

  @Override
  public Object call() {
    test(test);
    return null;
  }

  private void test(Test test) {
    bmStartTime = System.currentTimeMillis();
    lastPrintTime = bmStartTime;
    while (true) {
      try {
        long startTime = System.nanoTime();
        if (test == Test.GET) {
          get();
        } else if (test == Test.PUT) {
          put();
        } else {
          throw new UnsupportedOperationException("multi threading listing is not implemeneted " +
                  "yet");
        }
        long opExeTime = (System.nanoTime() - startTime);
        latency.addValue(opExeTime);
        successfulOps.incrementAndGet();
        printSpeed(test, bmStartTime, successfulOps);
      } catch (Throwable e) {
        failedOps.incrementAndGet();
        e.printStackTrace();
      } finally {
        if ((System.currentTimeMillis() - bmStartTime) > conf.getBenchmarkDuration()) {
          break;
        }
      }
    }
  }

  private void put() throws IOException {
    UUID objectKey = UUID.randomUUID();
    int bucketID = rand.nextInt(conf.getNumBuckets());
    String bucket = cloudConnector.getBucketDNSID(bucketID);
    String prefix = "";

    BucketObject obj = new BucketObject(bucket, prefix, objectKey.toString());
    Map<String, String> metadata = new HashMap<>();
    metadata.put("metadata1", "metadata1-value");
    metadata.put("metadata2", "metadata2-value");
    cloudConnector.uploadObject((short) bucketID, objectKey.toString(), tempFile, metadata);
    Namespace.put(obj);
  }

  private void createTempFile() throws IOException {
    UUID key = UUID.randomUUID();
    tempFile = new File(conf.getTmpFolder() + File.separator + key.toString());

    if (!tempFile.createNewFile()) {
      throw new RuntimeException("Unable to create temp file");
    }

    byte buffer[] = new byte[conf.getObjSize()];
    try (FileOutputStream fos = new FileOutputStream(tempFile)) {
      fos.write(buffer);
    }
  }


  private void get() {
    throw new UnsupportedOperationException("put not implemented yet");
  }

  static long lastPrintTime = System.currentTimeMillis();

  private synchronized static void printSpeed(Test test, long startTime,
                                              AtomicInteger successfulOps) {
    long curTime = System.currentTimeMillis();
    if ((curTime - lastPrintTime) > 5000) {
      long timeElapsed = (System.currentTimeMillis() - startTime);
      double speed = (successfulOps.get() / (double) timeElapsed) * 1000;
      System.out.println("Test: " + test + " Successful Ops: " + successfulOps + "\tSpeed: " + speed +
              " ops/sec.");
      lastPrintTime = System.currentTimeMillis();
    }
  }
}

