package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class LongTest extends BaseTest {
	private Long[] testLong = null;
	private String[] testLongString = null;
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
	public void testDoublePrecisionSetLongByUsingInsert() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testLongString = new String[] { Integer.MAX_VALUE + "", Integer.MIN_VALUE + "" };
			testLong = new Long[] { new Long(testLongString[0]), new Long(testLongString[1]) };
			insertLong(dml, testLong, false);

			checkLong(stmt, testDoubleTable, testLongString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetLongByUsingUpdate() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testLongString = new String[] { Integer.MAX_VALUE + "", Integer.MIN_VALUE + "" };
			testLong = new Long[] { new Long(testLongString[0]), new Long(testLongString[1]) };
			insertLong(updateDml, testLong, true);

			checkLong(stmt, testDoubleTable, testLongString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetLongByUsingUpsertInsert() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testLongString = new String[] { Integer.MAX_VALUE + "", Integer.MIN_VALUE + "" };
			testLong = new Long[] { new Long(testLongString[0]), new Long(testLongString[1]) };
			insertLong(upsertDml, testLong, false);

			checkLong(stmt, testDoubleTable, testLongString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetLongByUsingUpsertUpdate() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testLongString = new String[] { Integer.MAX_VALUE + "", Integer.MIN_VALUE + "" };
			testLong = new Long[] { new Long(testLongString[0]), new Long(testLongString[1]) };
			insertLong(upsertDml, testLong, false);

			checkLong(stmt, testDoubleTable, testLongString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testGetLong_negative() {
		String dml = null;

		try {
			stmt = conn.createStatement();
			dml = "insert into " + schema + "." + testDoubleTable + " values(1, " + Double.MAX_VALUE + ")";
			stmt.executeUpdate(dml);

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			try {
				assertNotNull(rs.getLong(2));
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
