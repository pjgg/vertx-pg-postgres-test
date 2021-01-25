package com.example.starter.resources;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;

public class PostgresqlResources implements TestResourceLifecycleManager {

  GenericContainer postgresContainer;
  static final String POSTGRES_USER = "test";
  static final String POSTGRES_PWD = "test";
  static final String POSTGRES_DB = "amadeus";
  Map<String, String> config = new HashMap<>();

  @Override
  public Map<String, String> start() {
    postgresContainer = new GenericContainer(DockerImageName.parse("quay.io/debezium/postgres:latest"))
      .withEnv("POSTGRES_USER", POSTGRES_USER)
      .withEnv("POSTGRES_PASSWORD", POSTGRES_PWD)
      .withEnv("POSTGRES_DB", POSTGRES_DB)
      .withExposedPorts(5432);

    postgresContainer.waitingFor(new HostPortWaitStrategy()).start();

    config.put("postgres.host", postgresContainer.getHost());
    config.put("postgres.port", String.valueOf(postgresContainer.getFirstMappedPort()));
    config.put("postgres.db", POSTGRES_DB);
    config.put("postgres.user", POSTGRES_USER);
    config.put("postgres.pwd", POSTGRES_PWD);

    return config;
  }

  @Override
  public void stop() {
    if (Objects.nonNull(postgresContainer)) postgresContainer.stop();
  }

  public String pgHost() {
    return config.get("postgres.host");
  }

  public String pgUser() {
    return config.get("postgres.user");
  }

  public String pgPassword() {
    return config.get("postgres.pwd");
  }

  public String pgDbName() {
    return config.get("postgres.db");
  }

  public int pgPort() {
    return Integer.valueOf(config.get("postgres.port"));
  }
}
