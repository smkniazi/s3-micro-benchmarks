package com.lc.s3.test;

import java.io.IOException;
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
  private SynchronizedDescriptiveStatistics latency = new SynchronizedDescriptiveStatistics();

  Random rand = new Random(System.currentTimeMillis());
  CloudPersistenceProvider cloudConnector = null;


  public void startApplication(String[] args) throws Exception {
    conf.parseArgs(args);
    cloudConnector = new CloudPersistenceProviderS3Impl(conf);

    if(conf.isDeleteExistingData()){
      cloudConnector.format();
    }
    cloudConnector.checkAllBuckets();

    test(Test.PUT);
//    test(Test.GET);
  }

  private void test(Test test) throws IOException, InterruptedException {
    long startTime = System.currentTimeMillis();
    startMicroBench(test);
    long totExeTime = (System.currentTimeMillis() - startTime);
    long avgSpeed = (long) (((double) successfulOps.get() / (double) totExeTime) * 1000);
    double avgLatency = latency.getMean() / 1000000;
    blueColoredText("Test: " + Test.PUT +
            " Avg Speed: " + avgSpeed +
            " Avg Latency: " + avgLatency +
            " Successful Ops: " + successfulOps +
            " Failed: " + failedOps);

  }

  public List<Worker> createWorkers(Test test) throws InterruptedException, IOException {
    List<Worker> workers = new ArrayList<Worker>();
    for (int i = 0; i < conf.getNumClients(); i++) {
      Worker worker = new Worker(test, successfulOps, failedOps, latency, conf, cloudConnector);
      workers.add(worker);
    }
    return workers;
  }

  public void startMicroBench(Test test) throws InterruptedException, IOException {
    ExecutorService executor = null;
    executor = Executors.newFixedThreadPool(conf.getNumClients());
    List workers = createWorkers(test);
    executor.invokeAll(workers); //blocking call
    executor.shutdown();
  }

  protected void redColoredText(String msg) {
    System.out.println((char) 27 + "[31m" + msg);
    System.out.print((char) 27 + "[0m");
  }

  protected static void blueColoredText(String msg) {
    System.out.println((char) 27 + "[36m" + msg);
    System.out.print((char) 27 + "[0m");
  }


}
