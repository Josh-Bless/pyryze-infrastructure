package com.pyryze.infrastructure.exception;

public class ExitReactPipelineException extends RuntimeException {

  private static final long serialVersionUID = 1L;
  private Object data = null;
  
  public ExitReactPipelineException(String msg, Object data) {
    super(msg);
    this.data = data;
  }
  
  @SuppressWarnings("unchecked")
  public <T> T getPayLoad(){
      return (T)data;
  }
}