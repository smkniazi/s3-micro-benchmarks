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

  public static Namespace createNamespace(Configuration config) {
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
      if (namespace.size() > 0) {
        return namespace.remove(namespace.size() - 1);
      }
      return null;
    }
  }

  public void load() throws IOException, ClassNotFoundException {
    System.out.println("Loading namespace from disk");

    File file = new File(conf.getDiskNSFile());
    if (file.exists()) {
      FileInputStream fileIn = new FileInputStream(file);
      ObjectInputStream objectIn = new ObjectInputStream(fileIn);
      blockID = (AtomicLong) objectIn.readObject();
      int objsRead = 0;
      try {
        do {
          Object obj = objectIn.readObject();
          if (obj != null) {
            namespace.add((BucketObject) obj);
            String msg = "\rObjects Read: " + (objsRead++);
            System.out.print(msg);
          }
        } while (true);
      } catch (EOFException e) {
      }
      objectIn.close();
      System.out.println("\nLoaded namespace from file. Blocks: " + namespace.size() + " Max ID: " + blockID.get());
    }
  }

  public void save() throws IOException {
    File file = new File(conf.getDiskNSFile());
    if (file.exists()) {
      file.delete();
    }

    FileOutputStream fileOut = new FileOutputStream(file);
    ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
    objectOut.writeObject(blockID);
    for (BucketObject obj : namespace) {
      objectOut.writeObject(obj);
    }
    objectOut.close();
    System.out.println("Saved namespace to file");
  }

  public BucketObject newBucketObject() {
    long id = 0;
    synchronized (namespace) {
      id = blockID.incrementAndGet();
    }

    String blockKey = Long.toString(id);
    if(conf.isUsePrefixes()){
      long prefix = id / conf.getPrefixSize();
      blockKey = "folder-" + Long.toString(prefix) + "/" + id;
    }

    if (conf.getNumBuckets() > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Too many buckets. Max: " + Short.MAX_VALUE);
    }


    short bucketID = (short) rand.nextInt(conf.getNumBuckets());

    Map<String, String> metadata = new HashMap<>();
    metadata.put("metadata1", "metadata1-value");
    metadata.put("metadata2", "metadata2-value");

    BucketObject obj = new BucketObject(bucketID, blockKey);
    obj.setMetadata(metadata);

    return obj;
  }

  public void deleteLocalCopy() {
    File file = new File(conf.getDiskNSFile());
    if (file.exists()) {
      file.delete();
    }
  }
}
