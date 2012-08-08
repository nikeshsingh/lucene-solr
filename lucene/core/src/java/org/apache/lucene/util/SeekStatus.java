package org.apache.lucene.util;

/** Represents returned result from a seek operation.
 *  If status is FOUND, then the precise term was found.
 *  If status is NOT_FOUND, then a different term was
 *  found.  If the status is END, the end of the iteration
 *  was hit. */
public enum SeekStatus {END, FOUND, NOT_FOUND}