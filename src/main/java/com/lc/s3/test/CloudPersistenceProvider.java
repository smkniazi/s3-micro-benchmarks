package com.lc.s3.test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface CloudPersistenceProvider {
  /*
  deletes all the bucket belonging to the user.
  This is only used for testing.
   */
  public void deleteAllBuckets(String prefix);

  /*
  Deletes all the buckets that are used by HopsFS
   */
  public void format();

  /*
  Check that all the buckets needed exist
  throws runtime exception if the buckets dont exists or not writable.
   */
  public void checkAllBuckets();

  public String getBucketDNSID(int ID);

  public void uploadObject(short bucketID, String objectID, File object,
                           Map<String, String> metadata) throws IOException;

  public boolean objectExists(short bucketID, String objectID)
          throws IOException;

  public Map<String, String> getUserMetaData(short bucketID, String objectID)
          throws IOException;

  public long getObjectSize(short bucketID, String objectID)
          throws IOException;

  public void downloadObject(short bucketID, String objectID, File path)
          throws IOException;

  public List<String> getAll() throws IOException;

  public void deleteObject(short bucketID, String objectID) throws IOException;

  public void shutdown();
}
