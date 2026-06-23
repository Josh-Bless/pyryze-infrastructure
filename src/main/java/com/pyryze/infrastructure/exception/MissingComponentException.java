package com.pyryze.infrastructure.exception;

public class MissingComponentException extends RuntimeException {
  
  private static final long serialVersionUID = 10L;
  
  public MissingComponentException(String msg) {
    super(msg);
  }
}