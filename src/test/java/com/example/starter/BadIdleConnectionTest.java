package com.example.starter;

import io.reactivex.Observable;
import io.vertx.core.Handler;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.pgclient.PgPool;
import io.vertx.reactivex.sqlclient.SqlConnection;
import io.vertx.sqlclient.PoolOptions;
import java.time.OffsetDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BadIdleConnectionTest extends AbstractVerticle {

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
    vertx.deployVerticle(new BadIdleConnectionTest(connectOptions));
  }

  public BadIdleConnectionTest(PgConnectOptions connectOptions) {
    this.connectOptions = connectOptions;
  }

  @Override
  public void start() {

    int poolSize = 10;
    client = PgPool.pool(Vertx.currentContext().owner(), connectOptions, new PoolOptions().setMaxSize(poolSize));

    AtomicInteger at = new AtomicInteger(0);
    Handler<Long> handler = l -> {
      System.out.println("Connection #" + at.incrementAndGet());
      Observable.range(1, 3)
        .flatMap(n -> client.rxGetConnection()
          .map(con -> con.closeHandler(v -> System.out.println("Connection closed")))
          .map(con -> con.exceptionHandler(e -> System.out.println("Connection exception: '" + e.getMessage() + "'")))
          .doOnError(er -> System.out.println("Error to connect: '" + er.getMessage() + "'"))
          .doAfterSuccess(SqlConnection::close)
          .flatMapObservable(con -> con
            .rxPrepare("SELECT CURRENT_TIMESTAMP")
            .doOnError(error -> System.out.println("Error on query: '" + error.getMessage() + "'"))
            .flatMapPublisher(st -> st.createStream(20).toFlowable())
            .map(row -> {
              System.out.println("Result 1: " + row.get(OffsetDateTime.class, 0));
              return row.get(OffsetDateTime.class, 0);
            })
            .toObservable()
          ))
        .toList()
        .subscribe(re -> System.out.println("Subscribe success"), er ->
          System.out.println("Subscribe error: " + er.getMessage()));
    };
    vertx.setPeriodic(1015, l -> handler.handle(l));
  }
}
