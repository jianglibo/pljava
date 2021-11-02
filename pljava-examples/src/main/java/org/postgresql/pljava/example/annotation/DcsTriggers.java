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
import static org.postgresql.pljava.annotation.Trigger.Constraint.*;
import static org.postgresql.pljava.annotation.Trigger.Event.*;
import static org.postgresql.pljava.annotation.Trigger.Scope.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.postgresql.pljava.TriggerData;
import org.postgresql.pljava.annotation.Function;
import org.postgresql.pljava.annotation.Trigger;

/**
 * Example creating a couple of tables, and a function to be called when
 * triggered by insertion into either table. In PostgreSQL 10 or later,
 * also create a function and trigger that uses transition tables.
 *<p>
 * This example relies on {@code implementor} tags reflecting the PostgreSQL
 * version, set up in the {@link ConditionalDDR} example. Constraint triggers
 * appear in PG 9.1, transition tables in PG 10.
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
public class DcsTriggers
{
	/**
	 * insert user name in response to a trigger.
	 */
	@Function(
		schema = "public",
		security = INVOKER,
		// trust = Trust.UNSANDBOXED,
		triggers = {
			@Trigger(called = BEFORE, scope = ROW, table = "dcs_plugin",
					 events = { INSERT, UPDATE } )
		})

	public static void insertUpdateDscPlugin(TriggerData td)
	throws SQLException
	{
		ResultSet nrs = td.getNew(); // expect NPE in a DELETE/STATEMENT trigger
		String table_description = nrs.getString("table_description");
		String envs = nrs.getString("envs");
		String vars = nrs.getString("vars");
		try {
			Map<String, Object> vm = new JSONObject(vars).toMap();
			Map<String, Object> em = new JSONObject(envs).toMap();
			Map<String, Object> tm = new JSONObject(table_description).toMap();
		} catch (JSONException e) {
			throw new SQLException(e.getMessage());
		}
		// nrs.updateString("envs", "bob");
		// Map<String, Object> json = new HashMap<>();
		// json.put("key", "value");
		// json.put("array", Arrays.asList("1", "2"));
		// nrs.updateString("executable_tpl", envs.getClass().getName() + "," + vars.getClass().getName() + ","); # always string.
		// nrs.updateString("executable_tpl", envs + "," + vars );
	}

	// public static void main(String[] args) {

	// }


}
