package com.lc.s3.test;

import java.io.Serializable;
import java.util.Map;

public class BucketObject implements Serializable {
  private String key;
  private short bucket;
  private Map<String, String> metadata;
  private long blockID;

  public BucketObject(short bucket, String key, long blockID) {
    this.key = key;
    this.bucket = bucket;
    this.blockID = blockID;
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

  public long getBlockID() {
    return blockID;
  }

  public short getBucket() {
    return bucket;
  }

  @Override
  public int hashCode() {
    return key.hashCode() + Short.hashCode(bucket) + Long.hashCode(blockID);
  }
}
