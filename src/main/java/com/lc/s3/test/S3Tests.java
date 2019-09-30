package com.lc.s3.test;

public enum S3Tests {
  PUT("PUT"),
  GET("GET"),
  EXISTS("EXISTS"),
  GET_METADATA("GET_METADATA"),
  DELETE("DELETE"),
  LIST("LIST");

  String name;

  S3Tests(String name) {
    this.name = name;
  }
}
