package com.lc.s3.test;

import com.amazonaws.services.s3.model.Bucket;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class Namespace {

  private static Namespace ns;
  private static AtomicLong blockID;
  private List<BucketObject> namespace = new ArrayList<>();
  private List<String> prefixes = new ArrayList<>();
  private int index = 0;
  private Random rand = new Random(System.currentTimeMillis());
  private static Configuration conf;
  private static long startID;

  private Namespace() {
  }

  public static Namespace createNamespace(Configuration config) {
    if (ns == null) {
      ns = new Namespace();
    }

    conf = config;
    startID = conf.getClientId() * 10000000;
    blockID = new AtomicLong(startID);
    ns.initPrefixes();
    return ns;
  }

  public void initPrefixes(){
    if(conf.isUsePrefixes()){
      for(int i = 0; i < conf.getNoOfPrefixes(); i++){
        UUID uuid = UUID.randomUUID();
        prefixes.add(uuid.toString()+"/");
      }
    }
  }

  public void put(BucketObject obj) {
    synchronized (namespace) {
      namespace.add(obj);
    }
  }

  public BucketObject getObject() {
    synchronized (namespace) {
      if (namespace.size() > 0) {
        if (index >= namespace.size()) {
          index = 0;
        }
        return namespace.get(index++);
      } else {
        return null;
      }
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

  public BucketObject getRandomObj() {
    synchronized (namespace) {
      int randind = rand.nextInt(namespace.size());
      BucketObject obj = namespace.get(randind);
      return obj;
    }
  }

  public BucketObject newBucketObject() {
    long id = 0;
    String prefix = "";
    synchronized (namespace) {
      id = blockID.incrementAndGet();
    }

    if (conf.isUsePrefixes()) {
      prefix = prefixes.get(rand.nextInt(prefixes.size()));
    }

    if (conf.getNumBuckets() > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Too many buckets. Max: " + Short.MAX_VALUE);
    }

    short bucketID = (short) rand.nextInt(conf.getNumBuckets());

    Map<String, String> metadata = new HashMap<>();
    metadata.put("metadata1", "metadata1-value");
    metadata.put("metadata2", "metadata2-value");

    BucketObject obj = new BucketObject(bucketID, prefix, id);
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
