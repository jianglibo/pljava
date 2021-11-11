/*
 * Copyright (c) 2004-2020 Tada AB and other contributors, as listed below.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the The BSD 3-Clause License
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Contributors:
 *   Tada AB
 *   Purdue University
 *   Chapman Flack
 */
package org.postgresql.pljava.example.annotation;

import static org.postgresql.pljava.annotation.Function.Security.*;
import static org.postgresql.pljava.annotation.Trigger.Called.*;
// import static org.postgresql.pljava.annotation.Trigger.Constraint.*;
import static org.postgresql.pljava.annotation.Trigger.Event.*;
import static org.postgresql.pljava.annotation.Trigger.Scope.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.postgresql.pljava.TriggerData;
import org.postgresql.pljava.annotation.Function;
import org.postgresql.pljava.annotation.Trigger;

/**
 * Example creating a couple of tables, and a function to be called when triggered by insertion into
 * either table. In PostgreSQL 10 or later, also create a function and trigger that uses transition
 * tables.
 *
 * <p>This example relies on {@code implementor} tags reflecting the PostgreSQL version, set up in
 * the {@link ConditionalDDR} example. Constraint triggers appear in PG 9.1, transition tables in PG
 * 10.
 */
/*
 * Note for another day: this would seem an excellent place to add a
 * regression test for github issue #134 (make sure invocations of a
 * trigger do not fail with SPI_ERROR_UNCONNECTED). However, any test
 * here that runs from the deployment descriptor will be running when
 * SPI is already connected, so a regression would not be caught.
 * A proper test for it will have to wait for a proper testing harness
 * invoking tests from outside PL/Java itself.
 */
public class DcsTriggers {

  private static String m_url = "jdbc:default:connection";
  // /**
  //  * insert user name in response to a trigger.
  //  */
  // @Function(
  // 	schema = "public",
  // 	security = INVOKER,
  // 	// trust = Trust.UNSANDBOXED,
  // 	triggers = {
  // 		@Trigger(called = BEFORE, scope = ROW, table = "dcs_plugin",
  // 				 events = { INSERT, UPDATE } )
  // 	})

  // public static void insertUpdateDscPlugin(TriggerData td)
  // throws SQLException
  // {

  // 	ResultSet nrs = td.getNew(); // expect NPE in a DELETE/STATEMENT trigger
  // 	int count = nrs.getMetaData().getColumnCount();
  // 	for(int i = 1; i <= count; i++) {
  // 		ResultSetMetaData rsmd = nrs.getMetaData();
  // 		System.out.println("----------------------------------------------------");
  // 		System.out.println("type: " + rsmd.getColumnType(i));
  // 		System.out.println("catelogName: " + rsmd.getCatalogName(i));
  // 		System.out.println("className: " + rsmd.getColumnClassName(i));
  // 		System.out.println("displaySize: " + rsmd.getColumnDisplaySize(i));
  // 		System.out.println("label: " + rsmd.getColumnLabel(i));
  // 		System.out.println("name: " + rsmd.getColumnName(i));
  // 		System.out.println("typeName: " + rsmd.getColumnTypeName(i));
  // 		System.out.println("precision: " + rsmd.getPrecision(i));
  // 		System.out.println("scale: " + rsmd.getScale(i));
  // 		System.out.println("schemaName: " + rsmd.getSchemaName(i));
  // 		System.out.println("tableName: " + rsmd.getTableName(i));
  // 	}
  // 	String table_description = nrs.getString("table_description");

  // 	System.out.println("envs object: " + nrs.getObject("envs"));
  // 	String envs = nrs.getString("envs");
  // 	String vars = nrs.getString("vars");
  // 	String secret = nrs.getString("secret");
  // 	System.out.println("table_description: " + table_description);
  // 	System.out.println("envs: " + envs);
  // 	System.out.println("vars: " + vars);
  // 	System.out.println("secret: " + secret);
  // 	try {
  // 		Map<String, Object> em = new JSONObject(envs).toMap();
  // 		Map<String, Object> vm = new JSONObject(vars).toMap();
  // 		Map<String, Object> tm = new JSONObject(table_description).toMap();
  // 	} catch (JSONException e) {
  // 		throw new SQLException(e.getMessage());
  // 	}
  // }

  private static final String TABLE_DESCRIPTION_ERROR_MESSAGE =
      "table_description must contain 'columns' field,and it's must be a JSON array, and cannot be"
          + " empty. every column must be a json object.conatains 'key', 'name', 'type' three"
          + " fields. the 'key' is boolean, the 'name' and 'type' must be string.";
  private static final Set<String> TABLE_DESCRIPTION_COLUMN_NAMES = new HashSet<>();

  static {
    TABLE_DESCRIPTION_COLUMN_NAMES.add("key");
    TABLE_DESCRIPTION_COLUMN_NAMES.add("name");
    TABLE_DESCRIPTION_COLUMN_NAMES.add("type");
  }

  private static final Set<String> POSSIBLE_TYPE_NAMES = new HashSet<>();

  static {
    POSSIBLE_TYPE_NAMES.add("string");
    POSSIBLE_TYPE_NAMES.add("int");
    POSSIBLE_TYPE_NAMES.add("bigint");
    POSSIBLE_TYPE_NAMES.add("double");
    POSSIBLE_TYPE_NAMES.add("timestamp");
    POSSIBLE_TYPE_NAMES.add("boolean");
  }

  private static final String COLUMN_MAP_ERROR_MESSAGE =
      "column_map field is a string -> string map. cannot be empty.";

  /** validate dcs_plugin */
  @Function(
      schema = "public",
      security = INVOKER,
      triggers = {
        @Trigger(
            called = BEFORE,
            scope = ROW,
            table = "dcs_plugin",
            events = {INSERT, UPDATE})
      })
  public static void validateDscPlugin(TriggerData td) throws SQLException {

    ResultSet nrs = td.getNew(); // expect NPE in a DELETE/STATEMENT trigger
    String table_description = nrs.getString("table_description");
    String envs = nrs.getString("envs");
    String vars = nrs.getString("vars");
    try {
      new JSONObject(envs);
    } catch (JSONException e) {
      throw new SQLException("*** env field must be a JSON object. or leave it default. ***");
    }
    try {
      new JSONObject(vars);
    } catch (JSONException e) {
      throw new SQLException("*** vars field must be a JSON object. or leave it default. ***");
    }
    try {
      JSONObject description = new JSONObject(table_description);
      JSONArray columns = description.getJSONArray("columns");
      if (columns.length() == 0) {
        throw new SQLException(TABLE_DESCRIPTION_ERROR_MESSAGE);
      }
      for (int i = 0; i < columns.length(); i++) {
        JSONObject column = columns.getJSONObject(i);
        if (!(column.keySet().size() == 3
            && column.keySet().containsAll(TABLE_DESCRIPTION_COLUMN_NAMES))) {
          throw new SQLException(TABLE_DESCRIPTION_ERROR_MESSAGE);
        }
        column.getBoolean("key");
        if (!POSSIBLE_TYPE_NAMES.contains(column.getString("type"))) {
          throw new SQLException(
              TABLE_DESCRIPTION_ERROR_MESSAGE
                  + " , possible type value are: string, int, bigint, double, timestamp, boolean");
        }
        ;
        column.getString("name");
      }
    } catch (JSONException e) {
      throw new SQLException(TABLE_DESCRIPTION_ERROR_MESSAGE + "*****" + e.getMessage());
    }
  }

  /** validate dcs_plugin_instance */
  @Function(
      schema = "public",
      security = INVOKER,
      triggers = {
        @Trigger(
            called = BEFORE,
            scope = ROW,
            table = "dcs_plugin_instance",
            events = {INSERT, UPDATE})
      })
  public static void validateDscPluginInstance(TriggerData td) throws SQLException {
    ResultSet nrs = td.getNew(); // expect NPE in a DELETE/STATEMENT trigger
    String column_map_str = nrs.getString("column_map");
    String vars = nrs.getString("vars");
    String cron = nrs.getString("cron");
    //     System.out.println("cron: " + cron);
    if (cron != null) {
      Instant now = Instant.now();
      CronExpression ce = CronExpression.create(cron);
      Instant next = ce.next(now, ZoneId.systemDefault());
      Instant next2 = ce.next(next, ZoneId.systemDefault());
      //       System.out.println("next: " + next + ", next2: " + next2);
      long gap = Duration.between(next, next2).toMinutes();
      //       System.out.println("gap: " + gap);
      if (gap < 10L) {
        throw new SQLException(
            "cron gap cannot be less than 10 minutes. current gap is:" + gap + "minutes.");
      }
    }

    try {
      new JSONObject(vars);
    } catch (JSONException e) {
      throw new SQLException("*** vars field must be a JSON object. or leave it default. ***");
    }
    try {
      JSONObject column_map = new JSONObject(column_map_str);

      if (column_map.length() == 0) {
        throw new SQLException(COLUMN_MAP_ERROR_MESSAGE);
      }

      List<String> cmap_values = new ArrayList<>();
      Iterator<String> cmap_iter = column_map.keys();

      while (cmap_iter.hasNext()) {
        String next = cmap_iter.next();
        cmap_values.add(column_map.getString(next));
      }

      Set<String> cmap_values_set = new HashSet<>(cmap_values);

      Connection conn = DriverManager.getConnection(m_url);
      Statement stmt = conn.createStatement();
      ResultSet rs =
          stmt.executeQuery(
              "SELECT table_description from dcs_plugin where id = " + nrs.getInt("dcs_plugin_id"));

      String table_descriptoin_str = rs.getString("table_description");
      JSONObject table_descriptoin = new JSONObject(table_descriptoin_str);
      JSONArray table_descriptoin_columns = table_descriptoin.getJSONArray("columns");
      Iterator<Object> iter = table_descriptoin_columns.iterator();
      /** the value of the map must in the table description. */
      Set<String> kuduTableColumnNames = new HashSet<>();
      while (iter.hasNext()) {
        JSONObject next = (JSONObject) iter.next();
        kuduTableColumnNames.add(next.getString("name"));
      }

      if (!cmap_values_set.equals(kuduTableColumnNames)) {
        throw new SQLException(COLUMN_MAP_ERROR_MESSAGE + "***** The column_map and table_description doent's match");
      }

    } catch (JSONException e) {
      throw new SQLException(COLUMN_MAP_ERROR_MESSAGE + "*****" + e.getMessage());
    }
  }
}
