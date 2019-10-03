package com.lc.s3.test;

public class Purge {
  public static void main(String argv[]) throws Exception {
    System.out.println("Deleting all the buckets");
    Purge purger = new Purge();
    purger.purge(argv);

//    Purge purger = new Purge();
//    String []args = {"-purgeBuckets","salman" };
//    purger.purge(args);
  }

  private void purge(String argv[]) throws Exception {
    final Configuration conf = new Configuration();

    conf.parseArgs(argv);

    System.out.println("This will delete all buckets starting with "+ conf.getPurgeBucketsPrefix());
    System.in.read();

    CloudPersistenceProviderS3Impl.getConnector(conf).deleteAllBuckets(conf.getPurgeBucketsPrefix());
  }
}
