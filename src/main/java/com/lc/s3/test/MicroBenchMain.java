package com.lc.s3.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.math3.stat.descriptive.SynchronizedDescriptiveStatistics;

public class MicroBenchMain {
  private final Configuration conf = new Configuration();
  private AtomicInteger successfulOps = new AtomicInteger(0);
  private AtomicInteger failedOps = new AtomicInteger(0);
  private static long lastOutput = 0;
  private Namespace namespace;
  private SynchronizedDescriptiveStatistics latency = new SynchronizedDescriptiveStatistics();
  Random rand = new Random(System.currentTimeMillis());
  private static DecimalFormat df2 = new DecimalFormat("#.##");
  ExecutorService executor = null;

  public void startApplication(String[] args) throws Exception {

    conf.parseArgs(args);

    namespace = Namespace.createNamespace(conf);
    executor = Executors.newFixedThreadPool(conf.getNumClients());
    notePramsToResultsFile();

    if (conf.isDeleteExistingData()) {
      CloudPersistenceProviderS3Impl.getConnector(conf).format();
      namespace.deleteLocalCopy();
    }

    CloudPersistenceProviderS3Impl.getConnector(conf).createBuckets();


    ynprompt();

    CloudPersistenceProviderS3Impl.getConnector(conf).checkAllBuckets();

    if (conf.isSaveNSToDisk()) {
      namespace.load();
    }

    System.out.println("Number of parallel clients: " + conf.getNumClients());
    System.out.println("Disable sharing connections " + conf.isDisableConnectorSharing());
    System.out.println("Disable transfer manager " + conf.isDisableS3TransferManager());

    runTests();

    if (conf.isSaveNSToDisk()) {
      namespace.save();
    }
    System.exit(0);
  }

  private void runTests() throws IOException, InterruptedException {
    if (conf.isTestPut()) {
      test(S3Tests.PUT);
    }

    if (conf.isTestGet()) {
      test(S3Tests.GET);
    }

    if (conf.isTestMetadata()) {
      test(S3Tests.GET_METADATA);
    }

    if (conf.isTestObjExists()) {
      test(S3Tests.EXISTS);
    }

    if (conf.isTestList()) {
      test(S3Tests.LIST);
    }

    if (conf.isTestDelete()) {
      test(S3Tests.DELETE);
    }
  }

  private void test(S3Tests test) throws IOException, InterruptedException {
    System.out.println("\nStarting Test : " + test);
    successfulOps = new AtomicInteger(0);
    failedOps = new AtomicInteger(0);
    long startTime = System.currentTimeMillis();
    startMicroBench(test);
    long totExeTime = (System.currentTimeMillis() - startTime);
    double avgSpeed = (((double) successfulOps.get() / (double) totExeTime) * 1000);
    double avgLatency = latency.getMean() / 1000000;
    String message = "Test: " + test +
            " Successful Ops: " + successfulOps +
            " Failed: " + failedOps +
            " Avg Latency: " + df2.format(avgLatency) + " ms" +
            " Avg Speed: " + df2.format(avgSpeed) + " ops/sec" ;
    blueColoredText(message);
    writeResult(message+"\n");
  }

  public List<Worker> createWorkers(S3Tests test) throws InterruptedException, IOException {
    List<Worker> workers = new ArrayList<Worker>();
    for (int i = 0; i < conf.getNumClients(); i++) {
      Worker worker = new Worker(test, successfulOps, failedOps, latency,
              conf, namespace);
      workers.add(worker);
    }
    return workers;
  }

  public void startMicroBench(S3Tests test) throws InterruptedException, IOException {
    List workers = createWorkers(test);
    executor.invokeAll(workers); //blocking call
  }

  protected void redColoredText(String msg) {
    System.out.println((char) 27 + "[31m" + msg);
    System.out.print((char) 27 + "[0m");
  }

  protected static void blueColoredText(String msg) {
    System.out.println((char) 27 + "[36m" + msg);
    System.out.print((char) 27 + "[0m");
  }

  private void ynprompt() throws IOException {
    if(conf.isStartPrompt()) {
      System.out.println("Press Enter to start ");
      System.in.read();
    }
  }

  private void notePramsToResultsFile() throws IOException {
    File results = new File(conf.getResultFile());
    if(!results.exists()){
      results.createNewFile();
    }
    BufferedWriter writer = new BufferedWriter(new FileWriter(results,true));
    writer.write(conf.getParams());
    writer.close();
  }

  private void writeResult(String res) throws IOException {
    File results = new File(conf.getResultFile());
    BufferedWriter writer = new BufferedWriter(new FileWriter(results, true));
    writer.write(res);
    writer.close();
  }

}
