package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class FloatTest extends BaseTest {
	private Float[] testFloat = null;
	private String[] testFloatString = null;

	private String minusApproachZero = "-1.17549435e-38";
	private String approachZero = "1.17549435e-38";

	private String REAL_MAX_VALUE = "3.40282347e+38";
	private String MINUS_REAL_MAX_VALUE = "-3.40282347e+38";
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
	public void testDoubleSetFloatByUsingInsertApproachZero() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testFloatString = new String[] { approachZero, minusApproachZero };
			testFloat = new Float[] { new Float(testFloatString[0]), new Float(testFloatString[1]) };
			insertFloat(dml, testFloat, false);

			checkFloat(stmt, testDoubleTable, testFloatString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetFloatByUsingInsert() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testFloatString = new String[] { REAL_MAX_VALUE, MINUS_REAL_MAX_VALUE };
			testFloat = new Float[] { new Float(testFloatString[0]), new Float(testFloatString[1]) };
			insertFloat(dml, testFloat, false);

			checkFloat(stmt, testDoubleTable, testFloatString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update
	@Test
	public void testDoubleSetFloatByUsingUpdateApproachZero() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testFloatString = new String[] { approachZero, minusApproachZero };
			testFloat = new Float[] { new Float(testFloatString[0]), new Float(testFloatString[1]) };
			insertFloat(updateDml, testFloat, true);

			checkFloat(stmt, testDoubleTable, testFloatString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetFloatByUsingUpdate() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testFloatString = new String[] { REAL_MAX_VALUE, MINUS_REAL_MAX_VALUE };
			testFloat = new Float[] { new Float(testFloatString[0]), new Float(testFloatString[1]) };
			insertFloat(updateDml, testFloat, true);

			checkFloat(stmt, testDoubleTable, testFloatString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert
	@Test
	public void testDoubleSetFloatByUsingUpsertInsertApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testFloatString = new String[] { approachZero, minusApproachZero };
			testFloat = new Float[] { new Float(testFloatString[0]), new Float(testFloatString[1]) };
			insertFloat(upsertDml, testFloat, false);

			checkFloat(stmt, testDoubleTable, testFloatString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetFloatByUsingUpsertInsert() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testFloatString = new String[] { REAL_MAX_VALUE, MINUS_REAL_MAX_VALUE };
			testFloat = new Float[] { new Float(testFloatString[0]), new Float(testFloatString[1]) };
			insertFloat(upsertDml, testFloat, false);

			checkFloat(stmt, testDoubleTable, testFloatString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update
	@Test
	public void testDoubleSetFloatByUsingUpsertUpdateApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testFloatString = new String[] { approachZero, minusApproachZero };
			testFloat = new Float[] { new Float(testFloatString[0]), new Float(testFloatString[1]) };
			insertFloat(upsertDml, testFloat, false);

			checkFloat(stmt, testDoubleTable, testFloatString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetFloatByUsingUpsertUpdate() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testFloatString = new String[] { REAL_MAX_VALUE, MINUS_REAL_MAX_VALUE };
			testFloat = new Float[] { new Float(testFloatString[0]), new Float(testFloatString[1]) };
			insertFloat(upsertDml, testFloat, false);

			checkFloat(stmt, testDoubleTable, testFloatString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleByGetFloat() {
		String dml = null;

		try {
			stmt = conn.createStatement();
			dml = "insert into " + schema + "." + testDoubleTable + " values(1, 88.7)";
			stmt.executeUpdate(dml);

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(Float.parseFloat("88.7") == rs.getFloat(2));
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}
}
