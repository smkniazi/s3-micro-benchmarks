package com.lc.s3.test;

import java.io.Serializable;
import java.util.Map;

public class BucketObject implements Serializable {
  private short bucket;
  private String prefix;
  private Map<String, String> metadata;
  private long blockID;

  public BucketObject(short bucket, String prefix, long blockID) {
    this.prefix = prefix;
    this.bucket = bucket;
    this.blockID = blockID;
  }

  public String getPrefix() {
    return prefix;
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

  public String getKey() {
    return prefix+blockID;
  }
  @Override
  public int hashCode() {
    return prefix.hashCode() + Short.hashCode(bucket) + Long.hashCode(blockID);
  }
}
