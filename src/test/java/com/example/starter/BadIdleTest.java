package com.example.starter;

import io.reactivex.Observable;
import io.reactivex.Single;
import io.vertx.core.Handler;
import io.vertx.core.impl.NoStackTraceThrowable;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.pgclient.PgPool;
import io.vertx.reactivex.sqlclient.Row;
import io.vertx.reactivex.sqlclient.RowSet;
import io.vertx.reactivex.sqlclient.SqlConnection;
import io.vertx.sqlclient.PoolOptions;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BadIdleTest extends AbstractVerticle {

  private final PgConnectOptions connectOptions;
  private PgPool client;

  public static void main(String[] args) {
    PgConnectOptions connectOptions = new PgConnectOptions()
      .setHost("127.0.0.1")
      .setPort(5432)
      .setDatabase("amadeus")
      .setUser("test")
      .setPassword("test")
      .setConnectTimeout(5000)
//				.setReconnectAttempts(1)
//				.setReconnectInterval(100)
      .setIdleTimeout(1000)
      .setIdleTimeoutUnit(TimeUnit.MILLISECONDS);
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new BadIdleTest(connectOptions));
  }

  public BadIdleTest(PgConnectOptions connectOptions) {
    this.connectOptions = connectOptions;
  }

  @Override
  public void start() {

    int poolSize = 10;
    client = PgPool.pool(Vertx.currentContext().owner(), connectOptions, new PoolOptions().setMaxSize(poolSize));

    AtomicInteger at = new AtomicInteger(0);
    Handler<Long> handler = l -> {
      System.out.println("###################################################: ");
      Observable.range(1, 3)
        .concatMap(n -> {
          System.out.println("Connection #" + at.incrementAndGet());
          return client.preparedQuery("SELECT CURRENT_TIMESTAMP")
            .rxExecute()
            .doOnError(error -> {
              System.out.println("Error: " + at.get());
              System.err.println("Error on query: '" + error.getMessage() + "'");
            })
            .map(RowSet::iterator)
            .map(iterator -> {
              if (iterator.hasNext()) {
                Row row = iterator.next();
                System.out.println("Result : " + at.get() + " : " + row.getOffsetDateTime(0));

                return row.getOffsetDateTime(0);
              } else {
                return null;
              }
            })
            .toObservable();
        }).toList().subscribe(re -> {
          System.out.println("Subscribe success: -> " + re.get(0));
        },
        Throwable::printStackTrace);
    };
    vertx.setPeriodic(1015, l -> handler.handle(l));
  }
}
