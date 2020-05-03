package io.openaristos.dominus.core.errors;

public class DominusException extends RuntimeException {
  public DominusException(String message, Throwable throwable) {
    super(message, throwable);
  }

  public DominusException(String message) {
    super(message);
  }


}
