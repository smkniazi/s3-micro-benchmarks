package com.lc.s3.test;

public enum Test {
  PUT("PUT"),
  GET("GET"),
  LIST("LIST");
  String name;

  Test(String name) {
    this.name = name;
  }
}
