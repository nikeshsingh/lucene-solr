package org.apache.lucene.index.log;

public enum LogOperation {
  
  ADD((byte)1), UPDATE((byte)2), DELETE_BY_QUERY((byte)3), DELETE_BY_TERM((byte)4), COMMIT((byte)5);
  private static final byte ADD_TYPE = 1;
  private static final byte UPDATE_TYPE = 2;
  private static final byte DEL_BY_QUERY_TYPE = 3;
  private static final byte DEL_BY_TERM_TYPE = 4;
  private static final byte COMMIT_TYPE = 5;

  
  private final byte type;

  private LogOperation(byte type) {
    this.type = type;
  }
  
  public byte type() {
    return type;
  }
  
  public LogOperation fromType(byte type) {
    switch (type) {
    case ADD_TYPE:
      return ADD;
    case UPDATE_TYPE:
      return UPDATE;
    case DEL_BY_QUERY_TYPE:
      return LogOperation.DELETE_BY_QUERY;
    case DEL_BY_TERM_TYPE:
      return DELETE_BY_TERM;
    case COMMIT_TYPE:
      return COMMIT;
    default:
      throw new IllegalArgumentException("unknown type " + type);
    }
  }
}
