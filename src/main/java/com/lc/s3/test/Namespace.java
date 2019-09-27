package com.lc.s3.test;

import com.amazonaws.services.s3.model.Bucket;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Namespace {

  static List<BucketObject> namespace = new ArrayList<>();
  static Random rand = new Random(System.currentTimeMillis());

  public static void put(BucketObject obj) {
    synchronized (namespace) {
      namespace.add(obj);
    }
  }

  public static BucketObject getRandom() {
    synchronized (namespace) {
      int index = rand.nextInt(namespace.size());
      return namespace.get(index);
    }
  }

  public static void load(String diskCopy) throws IOException, ClassNotFoundException {
    File file = new File(diskCopy);
    if(file.exists()){
      FileInputStream fileIn = new FileInputStream(file);
      ObjectInputStream objectIn = new ObjectInputStream(fileIn);
      namespace = (ArrayList<BucketObject>) objectIn.readObject();
      objectIn.close();
      System.out.println("Loaded namespace from file");
    }

  }

  public static void save(String diskCopy) throws IOException {
    File file = new File(diskCopy);
    if(file.exists()){
      file.delete();
    }

    FileOutputStream fileOut = new FileOutputStream(file);
    ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
    objectOut.writeObject(namespace);
    objectOut.close();
    System.out.println("Saved namespace to file");
  }
}
