package com.lc.s3.test;

import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class Worker implements Runnable {
  private final AtomicInteger successfulOps;
  private final AtomicInteger failedOps;
  private final SynchronizedDescriptiveStatistics latency;
  private final Random rand = new Random(System.nanoTime());
  private final Configuration conf;
  private final S3Tests test;
  private int counter = 0;
  private File tempPutFile;
  private File tempGetFile;
  private final Namespace namespace;
  private boolean run = true;
  private boolean workDone = false;

  public Worker(S3Tests test, AtomicInteger successfulOps, AtomicInteger failedOps,
                SynchronizedDescriptiveStatistics lagency, Configuration conf,
                Namespace namespace)
          throws IOException {
    this.successfulOps = successfulOps;
    this.failedOps = failedOps;
    this.latency = lagency;
    this.conf = conf;
    this.test = test;
    this.namespace = namespace;
    createTempFiles();
  }

  @Override
  public void run() {
    try {
      test(test);
    } finally {
      workDone = true;
    }
  }

  private void test(S3Tests test) {
    while (run) {
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
          if (!deleteTest()) {
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
      } catch (IOException e) {
        failedOps.incrementAndGet();
        e.printStackTrace();
      } catch (Throwable e) {
        failedOps.incrementAndGet();
        e.printStackTrace();
        throw e;
      } finally {
      }
    }
  }

  private void putTest() throws IOException {
    BucketObject obj = namespace.newBucketObject();
    Map<String, String> metadata = obj.getMetadata();
    if (conf.isDisableS3TransferManager()) {
      CloudPersistenceProviderS3Impl.getConnector(conf)
              .uploadObject(obj.getBucket(), obj.getKey(), tempPutFile, metadata);
    } else {
      CloudPersistenceProviderS3Impl.getConnector(conf)
              .uploadObjectUsingTM(obj.getBucket(), obj.getKey(), tempPutFile, metadata);
    }
    namespace.put(obj);
  }

  private void getTest() throws IOException {
    BucketObject obj = namespace.getObject();
    CloudPersistenceProviderS3Impl.getConnector(conf)
            .downloadObject(obj.getBucket(), obj.getKey(), tempGetFile);
  }

  private void existTest() throws IOException {
    BucketObject obj = namespace.getObject();
    if (!CloudPersistenceProviderS3Impl.getConnector(conf)
            .objectExists(obj.getBucket(), obj.getKey())) {
      System.err.println("Unexpected. Object not found");
    }
  }

  private boolean deleteTest() throws IOException {
    BucketObject obj = namespace.popLast();
    if (obj != null) {
      CloudPersistenceProviderS3Impl.getConnector(conf)
              .deleteObject(obj.getBucket(), obj.getKey());
      return true;
    }
    return false;
  }

  private void listTest() throws IOException {
    BucketObject obj = namespace.getRandomObj();
    String prefixStr = obj.getPrefix();
    int count = CloudPersistenceProviderS3Impl.getConnector(conf).listDir(obj.getBucket(),
            prefixStr );
    assert count > 0;
  }

  private void metadataTest() throws IOException {
    BucketObject obj = namespace.getObject();
    if (CloudPersistenceProviderS3Impl.getConnector(conf)
            .getUserMetaData(obj.getBucket(), obj.getKey()) == null) {
      System.err.println("Unexpected. Object not found.");
    }
  }

  private void createTempFiles() throws IOException {
    UUID key = UUID.randomUUID();
    tempPutFile = new File(conf.getTmpFolder() + File.separator + key.toString());

    if (!tempPutFile.createNewFile()) {
      throw new RuntimeException("Unable to create temp file. " + tempPutFile);
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

  public boolean isDead() {
    return workDone;
  }

  public void dieDieDie() {
    run = false;
  }
}

