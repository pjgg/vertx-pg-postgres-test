package com.example.starter.resources;

import java.util.Map;

public interface TestResourceLifecycleManager {

  /**
   * Start the test resource.
   *
   * @return A map of system properties that should be set for the running test
   */
  Map<String, String> start();

  /**
   * Stop the test resource.
   */
  void stop();
}
