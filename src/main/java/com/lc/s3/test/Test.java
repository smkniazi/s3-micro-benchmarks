package com.lc.s3.test;

public enum Test {
  PUT("PUT"),
  GET("GET"),
  EXISTS("EXISTS"),
  GET_METADATA("GET_METADATA"),
  DELETE("DELETE"),
  LIST("LIST");

  String name;

  Test(String name) {
    this.name = name;
  }
}
