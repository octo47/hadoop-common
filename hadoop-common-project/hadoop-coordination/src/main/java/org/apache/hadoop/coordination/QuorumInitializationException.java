package org.apache.hadoop.coordination;

/**
 * Exception thrown when CE unable to initialize itself.
 */
public class QuorumInitializationException extends NoQuorumException {
  public QuorumInitializationException(String message) {
    super(message);
  }

  public QuorumInitializationException(String message, Throwable t) {
    super(message, t);
  }

}
