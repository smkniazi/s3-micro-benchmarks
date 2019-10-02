package com.lc.s3.test;

public class Main {
  public static void main(String argv[]) throws Exception{
    new MicroBenchMain().startApplication(argv);
//    test();
  }

  private static void test() throws Exception {
    String []args = {"-deleteExistingData","-usePrefixes", "-prefixSize", "2", "-testList",
            "-prefixSize", "50"};
    new MicroBenchMain().startApplication(args);
  }
}
