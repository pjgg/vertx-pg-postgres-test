package com.example.starter;

import com.example.starter.resources.PostgresqlResources;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.SingleSource;
import io.vertx.reactivex.sqlclient.Row;
import io.vertx.reactivex.sqlclient.RowIterator;
import io.vertx.reactivex.sqlclient.RowSet;
import io.vertx.reactivex.sqlclient.Tuple;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.BeforeAll;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestMainVerticle {
  static final int TIMEOUT_SEC = 60;
  static PostgresqlResources postgres = new PostgresqlResources();

  @BeforeAll
  public static void beforeAll() {
    postgres.start();
  }

  @AfterAll
  public static void tearDown() {
    postgres.stop();
  }

  @Test
  @Order(1)
  void checkIdle() throws InterruptedException {
    int idle = 1;
    TestPgClient pgClient = TestPgClient.Builder.newInstance(postgres).withPoolSize(1).withIdle(idle, TimeUnit.SECONDS).build();
    CountDownLatch done = new CountDownLatch(1);
    activeConnections(pgClient).subscribe(activeCon-> {
      assertEquals(activeCon, 1, "postgres IDLE fail!.");
      done.countDown();
    });

    TimeUnit.SECONDS.sleep(idle);

    activeConnections(pgClient).subscribe(activeCon-> {
      assertEquals(activeCon, 1, "postgres IDLE fail!.");
      done.countDown();
    });

    done.await(TIMEOUT_SEC, TimeUnit.SECONDS);
    assertEquals(done.getCount(), 0, String.format("Idle too long, must not be grater than %d sec.", TIMEOUT_SEC));
  }

  @Test
  @DisplayName("⏱ DB connections are re-used")
  @Order(2)
  void checkDbPoolTurnover() throws Throwable {
    TestPgClient pgClient = TestPgClient.Builder.newInstance(postgres).withPoolSize(5).withIdle(3, TimeUnit.SECONDS).build();
    final int events = 250000;
    CountDownLatch done = new CountDownLatch(events);

    createDefaultTable(pgClient)
      .concatWith(loadDefaultData(pgClient))
      .subscribe(()->checkDbPoolTurnover(pgClient, done));

    done.await(TIMEOUT_SEC, TimeUnit.SECONDS);
    assertEquals(done.getCount(), 0, String.format("Missing %d events.", events - done.getCount()));
  }

  @Test
  @DisplayName("⏱ DB connections collisions")
  @Order(3)
  void checkCollisions() throws Throwable {
    TestPgClient pgClient = TestPgClient.Builder.newInstance(postgres).withPoolSize(10).withIdle(1, TimeUnit.SECONDS).build();
    final int events = 250000;
    CountDownLatch done = new CountDownLatch(events);

    for(int i = 0; i< events; i++) {
      pgClient.getPool().preparedQuery("SELECT CURRENT_TIMESTAMP").rxExecute()
        .doOnError(error -> System.err.println("Error on query: '" + error.getMessage() + "'"))
        .map(RowSet::iterator)
        .map(iterator -> {
          if(iterator.hasNext()){
            Row row = iterator.next();
            System.out.println("Result 1: " + row.getOffsetDateTime(0));
            done.countDown();
            return row.getOffsetDateTime(0);
          }else return null;
        }).toObservable().toList().subscribe(re -> System.out.println("Subscribe success"), er -> System.err.println("Subscribe error: " + er.getMessage()));
    }

    done.await(TIMEOUT_SEC, TimeUnit.SECONDS);
    assertEquals(done.getCount(), 0, String.format("Missing %d events.", events - done.getCount()));
  }

  private void checkDbPoolTurnover(TestPgClient pgClient, CountDownLatch done) throws InterruptedException {
    List<Completable> completed = new ArrayList<>();

    for(int i = 0; i< done.getCount(); i++){
      Single<Boolean> connectionsReUsed = Airline.findAll(pgClient.getPool())
        .flatMapCompletable(resultSet -> Completable.complete())
        .andThen(activeConnections(pgClient))
        .map(activeCon -> checkDbActiveConnections(activeCon, pgClient.getPoolOptions().getMaxSize()));

      completed.add(connectionsReUsed.flatMapCompletable(reUsed -> {
        assertTrue(reUsed, "More postgres SQL connections than pool pool max-size property");
        done.countDown();
        return Completable.complete();
      }));
    }

    Completable.merge(completed).subscribe();
  }

  private boolean checkDbActiveConnections(long activeCon, int poolSize) {
    return activeCon <= poolSize;
  }

  private Single<Long> activeConnections(TestPgClient pgClient) {
    return pgClient.getPool().preparedQuery("SELECT count(*) as active_con FROM pg_stat_activity where application_name = 'vertx-pg-client'").rxExecute()
      .map(RowSet::iterator)
      .map(iterator -> iterator.hasNext() ? iterator.next().getLong("active_con") : null);
  }

  private Function<RowIterator<Row>, SingleSource<List<Airline>>> processAirlineResp() {
    return iterator -> {
      List<Airline> airlines = new ArrayList<>();
      while (iterator.hasNext()) {
        airlines.add(Airline.from(iterator.next()));
      }

      return Single.just(airlines);
    };
  }

  private Completable createDefaultTable(TestPgClient pgClient){
    return pgClient.getPool().preparedQuery("CREATE TABLE airlines (\n" +
      "  id            SERIAL PRIMARY KEY,\n" +
      "  iata_code     VARCHAR(100) NOT NULL UNIQUE,\n" +
      "  name          VARCHAR(100) NOT NULL,\n" +
      "  infant_price  FLOAT(3)\n" +
      ");").rxExecute().flatMapCompletable(RowSet -> Completable.complete());
  }

  private Completable loadDefaultData(TestPgClient pgClient){

    List<Tuple> batch = new ArrayList<>();
    batch.add(Tuple.of("IB","Iberia",10));
    batch.add(Tuple.of("BA","British Airways",15));
    batch.add(Tuple.of("LH","Lufthansa",7));
    batch.add(Tuple.of("FR","Ryanair",20));
    batch.add(Tuple.of("VY","Vueling",10));
    batch.add(Tuple.of("TK","Turkish Airlines", 5));

    return pgClient.getPool()
      .preparedQuery("INSERT INTO airlines (iata_code, name, infant_price) VALUES ($1, $2, $3)").rxExecuteBatch(batch).flatMapCompletable(res-> Completable.complete());
  }

}
