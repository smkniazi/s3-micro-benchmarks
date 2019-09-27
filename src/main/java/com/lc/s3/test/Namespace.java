package com.lc.s3.test;

import com.amazonaws.services.s3.model.Bucket;

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
}
