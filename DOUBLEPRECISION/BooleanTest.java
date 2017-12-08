package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class BooleanTest extends BaseTest {
	private Boolean testBoolean = null;
	private static String testDoubleTable = "testDoubleTable";
	
	@BeforeClass
	public static void create() {
		Statement createTableStmt = null;
		try {
			createTableStmt = conn.createStatement();
			createTableStmt.executeUpdate("create table IF NOT EXISTS " + schema + "." + testDoubleTable
					+ " (c_id int not null,c_double DOUBLE PRECISION, primary key(c_id) )");

			createTableStmt.executeUpdate("delete from " + schema + "." + testDoubleTable);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (createTableStmt != null) {
				try {
					createTableStmt.close();
					createTableStmt = null;
				} catch (SQLException e) {
					System.out.println(e.getMessage());
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void testDoublePrecisionSetBooleanByUsingInsertTrue() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBoolean = true;
			insertBoolean(dml, testBoolean, false);

			checkBoolean(stmt, testDoubleTable, testBoolean);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBooleanByUsingInsertFalse() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBoolean = false;
			insertBoolean(dml, testBoolean, false);

			checkBoolean(stmt, testDoubleTable, testBoolean);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBooleanByUsingUpdateTrue() {
		String dml = null;
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double= ? where c_ID = ?";

		try {
			stmt = conn.createStatement();
			dml = "insert into " + schema + "." + testDoubleTable + " values(1, 88.7)";
			stmt.executeUpdate(dml);

			testBoolean = true;
			insertBoolean(updateDml, testBoolean, true);

			checkBoolean(stmt, testDoubleTable, testBoolean);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBooleanByUsingUpdateFalse() {
		String dml = null;
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double= ? where c_ID = ?";

		try {
			stmt = conn.createStatement();
			dml = "insert into " + schema + "." + testDoubleTable + " values(1, 88.7)";
			stmt.executeUpdate(dml);

			testBoolean = false;
			insertBoolean(updateDml, testBoolean, true);

			checkBoolean(stmt, testDoubleTable, testBoolean);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBooleanByUsingUpsertInsertTrue() {
		String upsertDml = "upsert using load into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBoolean = true;
			insertBoolean(upsertDml, testBoolean, false);

			checkBoolean(stmt, testDoubleTable, testBoolean);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBooleanByUsingUpsertInsertFalse() {
		String upsertDml = "upsert using load into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBoolean = true;
			insertBoolean(upsertDml, testBoolean, false);

			checkBoolean(stmt, testDoubleTable, testBoolean);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBooleanByUsingUpsertUpdateTrue() {
		String dml = null;
		String updateDml = "upsert using load into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();
			dml = "insert into " + schema + "." + testDoubleTable + " values(1, 88.7)";
			stmt.executeUpdate(dml);

			testBoolean = true;
			insertBoolean(updateDml, testBoolean, false);

			checkBoolean(stmt, testDoubleTable, testBoolean);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBooleanByUsingUpsertUpdateFalse() {
		String dml = null;
		String updateDml = "upsert using load into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();
			dml = "insert into " + schema + "." + testDoubleTable + " values(1, 88.7)";
			stmt.executeUpdate(dml);

			testBoolean = false;
			insertBoolean(updateDml, testBoolean, false);

			checkBoolean(stmt, testDoubleTable, testBoolean);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testGetBoolean_negative() {
		String dml = null;

		try {
			stmt = conn.createStatement();
			dml = "insert into " + schema + "." + testDoubleTable + " values(1, 88.7)";
			stmt.executeUpdate(dml);

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			try {
				assertTrue(rs.getBoolean(2));
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}
}
