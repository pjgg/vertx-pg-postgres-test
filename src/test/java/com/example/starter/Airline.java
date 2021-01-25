package com.example.starter;

import io.reactivex.Single;
import io.vertx.reactivex.pgclient.PgPool;
import io.vertx.reactivex.sqlclient.Row;
import io.vertx.reactivex.sqlclient.RowSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class Airline {
  private static final String QUALIFIED_CODE_NAME = "iata_code";
  private String code;

  private static final String QUALIFIED_NAME_NAME = "name";
  private String name;

  private static final String QUALIFIED_INFANT_PRICE_NAME = "infant_price";
  private double infantPrice;

  public Airline(String code, String name, double infantPrice) {
    this.code =code;
    this.name= name;
    this.infantPrice = infantPrice;
  }

  public Airline(){}

  public static Airline from(Row row) {
    return new Airline(row.getString(QUALIFIED_CODE_NAME), row.getString(QUALIFIED_NAME_NAME), row.getFloat(QUALIFIED_INFANT_PRICE_NAME));
  }

  protected static Single<List<Airline>> fromSet(RowSet<Row> rows) {
      Iterator<Row> iterator = rows.iterator();
      List<Airline> airlines = new ArrayList<>();
      while(iterator.hasNext()) {
        airlines.add(Airline.from(iterator.next()));
      }

      return Single.just(airlines);
  }

  public static Single<List<Airline>> findAll(PgPool client) {
    return client.preparedQuery("SELECT * FROM airlines").rxExecute().flatMap(Airline::fromSet);
  }

  @Override
  public String toString() {
    return "Airline{" +
      "code='" + code + '\'' +
      ", name='" + name + '\'' +
      ", infantPrice=" + infantPrice +
      '}';
  }
}
