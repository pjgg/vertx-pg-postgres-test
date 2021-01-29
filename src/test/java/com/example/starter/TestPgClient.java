package com.example.starter;

import com.example.starter.resources.PostgresqlResources;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.reactivex.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import java.util.concurrent.TimeUnit;

public class TestPgClient {

  private final PgPool pool;
  private final PgConnectOptions connectOptions;
  private final PoolOptions poolOptions;

  public TestPgClient(Builder builder){
    pool = PgPool.pool(builder.pgConnectOptions, builder.poolOptions);
    connectOptions = builder.pgConnectOptions;
    connectOptions.setReconnectAttempts(3);
    poolOptions = builder.poolOptions;
  }

  public PgPool getPool() {
    return pool;
  }

  public PgConnectOptions getConnectOptions() {
    return connectOptions;
  }

  public PoolOptions getPoolOptions() {
    return poolOptions;
  }

  public static class Builder {

    private PgConnectOptions pgConnectOptions;
    private PoolOptions poolOptions;

    public static Builder newInstance(PostgresqlResources postgres){ return new Builder(postgres);}

    private Builder(PostgresqlResources postgres) {
      this.poolOptions = new PoolOptions();
      this.pgConnectOptions = new PgConnectOptions()
        .setPort(postgres.pgPort())
        .setHost(postgres.pgHost())
        .setDatabase(postgres.pgDbName())
        .setUser(postgres.pgUser())
        .setPassword(postgres.pgPassword());
    }

    public Builder withPoolSize(int poolSize) {
      poolOptions.setMaxSize(poolSize);
      return this;
    }

    public Builder withIdle(int idle, TimeUnit unit) {
      pgConnectOptions.setIdleTimeout(idle);
      pgConnectOptions.setIdleTimeoutUnit(unit);
      return this;
    }

    public TestPgClient build(){return new TestPgClient(this);}
  }
}
