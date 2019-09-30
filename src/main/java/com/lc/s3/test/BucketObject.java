package com.lc.s3.test;

import java.io.Serializable;
import java.util.Map;

public class BucketObject implements Serializable {
  private String key;
  private short bucket;
  private String prefix;
  private Map<String, String> metadata;

  public BucketObject(short bucket, String prefix, String key) {
    this.key = key;
    this.bucket = bucket;
    this.prefix = prefix;
  }

  public String getKey() {
    return key;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public short getBucket() {
    return bucket;
  }

  public String getPrefix() {
    return prefix;
  }

  @Override
  public int hashCode() {
    return key.hashCode() + Short.hashCode(bucket) + prefix.hashCode();
  }
}
