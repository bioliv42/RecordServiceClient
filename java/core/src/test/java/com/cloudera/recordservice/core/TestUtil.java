package com.cloudera.recordservice.core;

import java.io.File;
import java.io.IOException;

/**
 * Common utilities to use for testing.
 */
public class TestUtil {
  /**
   * Returns the absolute path of a temp directory. The directory is *not* created.
   */
  public static String getTempDirectory() throws IOException {
    final File temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
    if (!(temp.delete())) {
      throw new IOException("Could not delete temp file: " + temp.getAbsolutePath());
    }
    return temp.getAbsolutePath();
  }
}
