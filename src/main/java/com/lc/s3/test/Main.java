package com.lc.s3.test;

public class Main {
  public static void main(String argv[]) throws Exception{
    new MicroBenchMain().startApplication(argv);
//    test();
  }

  private static void test() throws Exception {
    String []args = {"-deleteExistingData","-usePrefixes", "-noOfPrefixes", "2", "-testList",
            };
    new MicroBenchMain().startApplication(args);
  }
}
