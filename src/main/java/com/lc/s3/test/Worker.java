package com.lc.s3.test;

import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class Worker implements Callable {
  private final AtomicInteger successfulOps;
  private final AtomicInteger failedOps;
  private final SynchronizedDescriptiveStatistics latency;
  private final Random rand = new Random(System.nanoTime());
  private final Configuration conf;
  private final CloudPersistenceProvider cloudConnector;
  private final S3Tests test;
  private int counter = 0;
  private long bmStartTime = 0;
  private File tempPutFile;
  private File tempGetFile;
  private final Namespace namespace;

  public Worker(S3Tests test, AtomicInteger successfulOps, AtomicInteger failedOps,
                SynchronizedDescriptiveStatistics lagency, Configuration conf,
                CloudPersistenceProvider cloudConnector, Namespace namespace)
          throws IOException {
    this.successfulOps = successfulOps;
    this.failedOps = failedOps;
    this.latency = lagency;
    this.conf = conf;
    this.cloudConnector = cloudConnector;
    this.test = test;
    this.namespace = namespace;
    createTempFiles();
  }

  @Override
  public Object call() {
    test(test);
    return null;
  }

  private void test(S3Tests test) {
    bmStartTime = System.currentTimeMillis();
    lastPrintTime = bmStartTime;
    while (true) {
      try {
        long startTime = System.nanoTime();
        if (test == S3Tests.PUT) {
          putTest();
        } else if (test == S3Tests.GET) {
          getTest();
        } else if (test == S3Tests.GET_METADATA) {
          metadataTest();
        } else if (test == S3Tests.EXISTS) {
          existTest();
        } else if (test == S3Tests.DELETE) {
          if (deleteTest()){
            return;
          }
        } else if (test == S3Tests.LIST) {
          listTest();
        } else {
          throw new UnsupportedOperationException("Test not implemented yet");
        }
        long opExeTime = (System.nanoTime() - startTime);
        latency.addValue(opExeTime);
        successfulOps.incrementAndGet();
        printSpeed(test, bmStartTime, successfulOps);
      } catch (IOException e) {
        failedOps.incrementAndGet();
        e.printStackTrace();
      } finally {
        if ((System.currentTimeMillis() - bmStartTime) > conf.getBenchmarkDuration()) {
          break;
        }
      }
    }
  }

  private void putTest() throws IOException {
    BucketObject obj = namespace.newBucketObject();
    Map<String, String> metadata = obj.getMetadata();
    cloudConnector.uploadObject(obj.getBucket(), obj.getKey(), tempPutFile, metadata);
    namespace.put(obj);
  }

  private void getTest() throws IOException {
    BucketObject obj = namespace.getRandomObject();
    cloudConnector.downloadObject(obj.getBucket(), obj.getKey(), tempGetFile);
  }

  private void existTest() throws IOException {
    BucketObject obj = namespace.getRandomObject();
    if(!cloudConnector.objectExists(obj.getBucket(), obj.getKey())){
      System.err.println("Unexpected. Object not found");
    }
  }

  private boolean deleteTest() throws IOException {
    BucketObject obj = namespace.popLast();
    if( obj != null) {
      cloudConnector.deleteObject(obj.getBucket(), obj.getKey());
      return true;
    }
    return false;
  }

  private void listTest() throws IOException {
    IOException up = new IOException("not implemented yet");
    throw up;
  }

  private void metadataTest() throws IOException {
    BucketObject obj = namespace.getRandomObject();
    if(cloudConnector.getUserMetaData(obj.getBucket(), obj.getKey()) == null){
      System.err.println("Unexpected. Object not found.");
    }
  }

  private void createTempFiles() throws IOException {
    UUID key = UUID.randomUUID();
    tempPutFile = new File(conf.getTmpFolder() + File.separator + key.toString());

    if (!tempPutFile.createNewFile()) {
      throw new RuntimeException("Unable to create temp file. "+tempPutFile);
    }

    byte buffer[] = new byte[conf.getObjSize()];
    try (FileOutputStream fos = new FileOutputStream(tempPutFile)) {
      fos.write(buffer);
    }

    key = UUID.randomUUID();
    tempGetFile = new File(conf.getTmpFolder() + File.separator + key.toString());
    if (!tempGetFile.createNewFile()) {
      throw new IOException("Unable to create temp file " + tempGetFile);
    }
  }



  static long lastPrintTime = System.currentTimeMillis();

  private synchronized static void printSpeed(S3Tests test, long startTime,
                                              AtomicInteger successfulOps) {
    long curTime = System.currentTimeMillis();
    if ((curTime - lastPrintTime) > 5000) {
      long timeElapsed = (System.currentTimeMillis() - startTime);
      double speed = (successfulOps.get() / (double) timeElapsed) * 1000;
      System.out.println("Test: " + test + " Successful Ops: " + successfulOps + "\tSpeed: " + (long)speed +
              " ops/sec.");
      lastPrintTime = System.currentTimeMillis();
    }
  }
}

