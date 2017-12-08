package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShortTest extends BaseTest {
	private Short[] testShort = null;
	private String[] testShortString = null;
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
	public void testDoublePrecisionSetShortByUsingInsert() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testShortString = new String[] { Short.MAX_VALUE + "", Short.MIN_VALUE + "" };
			testShort = new Short[] { new Short(testShortString[0]), new Short(testShortString[1]) };
			insertShort(dml, testShort, false);

			checkShort(stmt, testDoubleTable, testShortString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetShortByUsingUpdate() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testShortString = new String[] { Short.MAX_VALUE + "", Short.MIN_VALUE + "" };
			testShort = new Short[] { new Short(testShortString[0]), new Short(testShortString[1]) };
			insertShort(updateDml, testShort, true);

			checkShort(stmt, testDoubleTable, testShortString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetShortByUsingUpsertInsert() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testShortString = new String[] { Short.MAX_VALUE + "", Short.MIN_VALUE + "" };
			testShort = new Short[] { new Short(testShortString[0]), new Short(testShortString[1]) };
			insertShort(upsertDml, testShort, false);

			checkShort(stmt, testDoubleTable, testShortString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetShortByUsingUpsertUpdate() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testShortString = new String[] { Short.MAX_VALUE + "", Short.MIN_VALUE + "" };
			testShort = new Short[] { new Short(testShortString[0]), new Short(testShortString[1]) };
			insertShort(upsertDml, testShort, false);

			checkShort(stmt, testDoubleTable, testShortString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testGetShort_negative() {
		String dml = null;

		try {
			stmt = conn.createStatement();
			dml = "insert into " + schema + "." + testDoubleTable + " values(1, 1324567988)";
			stmt.executeUpdate(dml);

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			try {
				assertNotNull(rs.getShort(2));
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
