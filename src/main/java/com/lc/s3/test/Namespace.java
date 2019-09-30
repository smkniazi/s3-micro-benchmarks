package com.lc.s3.test;

import com.amazonaws.services.s3.model.Bucket;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class Namespace {

  private static Namespace ns;
  private static AtomicLong blockID;
  private List<BucketObject> namespace = new ArrayList<>();
  private Random rand = new Random(System.currentTimeMillis());
  private static Configuration conf;

  private Namespace() {
  }

  public static Namespace getNamespace(Configuration config) {
    if (ns == null) {
      ns = new Namespace();
    }

    conf = config;
    blockID = new AtomicLong(conf.getClientId() * 10000000);
    return ns;
  }

  public void put(BucketObject obj) {
    synchronized (namespace) {
      namespace.add(obj);
    }
  }

  public BucketObject getRandomObject() {
    synchronized (namespace) {
      int index = rand.nextInt(namespace.size());
      return namespace.get(index);
    }
  }

  public BucketObject popLast() {
    synchronized (namespace) {
      return namespace.remove(namespace.size() - 1);
    }
  }

  public void load(String diskCopy) throws IOException, ClassNotFoundException {
    File file = new File(diskCopy);
    if (file.exists()) {
      FileInputStream fileIn = new FileInputStream(file);
      ObjectInputStream objectIn = new ObjectInputStream(fileIn);
      blockID = (AtomicLong) objectIn.readObject();
      namespace = (ArrayList<BucketObject>) objectIn.readObject();
      objectIn.close();
      System.out.println("Loaded namespace from file");
    }
  }

  public void save(String diskCopy) throws IOException {
    File file = new File(diskCopy);
    if (file.exists()) {
      file.delete();
    }

    FileOutputStream fileOut = new FileOutputStream(file);
    ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
    objectOut.writeObject(blockID);
    objectOut.writeObject(namespace);
    objectOut.close();
    System.out.println("Saved namespace to file");
  }

  public BucketObject newBucketObject() {
    long id = 0;
    synchronized (namespace) {
      id = blockID.incrementAndGet();
    }

    if (conf.getNumBuckets() > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Too many buckets. Max: " + Short.MAX_VALUE);
    }

    short bucketID = (short) rand.nextInt(conf.getNumBuckets());
    String prefix = "";
    Map<String, String> metadata = new HashMap<>();
    metadata.put("metadata1", "metadata1-value");
    metadata.put("metadata2", "metadata2-value");

    BucketObject obj = new BucketObject(bucketID, prefix, Long.toString(id));
    obj.setMetadata(metadata);

    return obj;
  }
}
