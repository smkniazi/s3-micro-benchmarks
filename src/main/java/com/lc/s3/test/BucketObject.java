package com.lc.s3.test;

import java.io.Serializable;

public class BucketObject implements Serializable {
  private String key;
  private String bucket;
  private String prefix;

  public BucketObject(String bucket, String prefix, String key) {
    this.key = key;
    this.bucket = bucket;
    this.prefix = prefix;
  }

  public String getKey() {
    return key;
  }

  public String getBucket() {
    return bucket;
  }

  public String getPrefix() {
    return prefix;
  }

  @Override
  public int hashCode() {
    return key.hashCode() + bucket.hashCode() + prefix.hashCode();
  }
}
