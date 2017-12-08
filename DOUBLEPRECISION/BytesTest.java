package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class BytesTest extends BaseTest {

	private byte[][] testBytes = null;
	private String[] testBytesStringArray = null;

	private String minusApproachZero = "-2.2250738585072014E-308";
	private String approachZero = "2.2250738585072014E-308";

	private String DOUBLE_MAX_VALUE = Double.MAX_VALUE + "";
	private String MINUS_DOUBLE_MAX_VALUE = "-" + Double.MAX_VALUE + "";
	
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
	public void testDoublePrecisionSetBytesByUsingInsertApproachZero() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBytesStringArray = new String[] { approachZero, minusApproachZero };
			testBytes = new byte[][] { testBytesStringArray[0].getBytes(), testBytesStringArray[1].getBytes() };
			try {
				insertBytes(dml, testBytes, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Restricted data type attribute violation")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBytes insert value to DoublePrecision signed column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBytesByUsingInsert() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBytesStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBytes = new byte[][] { testBytesStringArray[0].getBytes(), testBytesStringArray[1].getBytes() };
			try {
				insertBytes(dml, testBytes, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Restricted data type attribute violation")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBytes insert value to DoublePrecision signed column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update
	@Test
	public void testDoublePrecisionSetBytesByUsingUpdateApproachZero() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBytesStringArray = new String[] { approachZero, minusApproachZero };
			testBytes = new byte[][] { testBytesStringArray[0].getBytes(), testBytesStringArray[1].getBytes() };
			try {
				insertBytes(updateDml, testBytes, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("Restricted data type attribute violation")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBytes update value to DoublePrecision signed column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBytesByUsingUpdate() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBytesStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBytes = new byte[][] { testBytesStringArray[0].getBytes(), testBytesStringArray[1].getBytes() };
			try {
				insertBytes(updateDml, testBytes, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("Restricted data type attribute violation")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBytes update value to DoublePrecision signed column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert
	@Test
	public void testDoublePrecisionSetBytesByUsingUpsertInsertApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testBytesStringArray = new String[] { approachZero, minusApproachZero };
			testBytes = new byte[][] { testBytesStringArray[0].getBytes(), testBytesStringArray[1].getBytes() };
			try {
				insertBytes(upsertDml, testBytes, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Restricted data type attribute violation")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBytes upsertinsert value to DoublePrecision signed column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBytesByUsingUpsertInsert() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testBytesStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBytes = new byte[][] { testBytesStringArray[0].getBytes(), testBytesStringArray[1].getBytes() };
			try {
				insertBytes(upsertDml, testBytes, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Restricted data type attribute violation")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBytes upsertinsert value to DoublePrecision signed column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update
	@Test
	public void testDoublePrecisionSetBytesByUsingUpsertUpdateApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBytesStringArray = new String[] { approachZero, minusApproachZero };
			testBytes = new byte[][] { testBytesStringArray[0].getBytes(), testBytesStringArray[1].getBytes() };
			try {
				insertBytes(upsertDml, testBytes, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Restricted data type attribute violation")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBytes upsertupdate value to DoublePrecision signed column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBytesByUsingUpsertUpdate() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBytesStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBytes = new byte[][] { testBytesStringArray[0].getBytes(), testBytesStringArray[1].getBytes() };
			try {
				insertBytes(upsertDml, testBytes, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Restricted data type attribute violation")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBytes upertupdate value to DoublePrecision signed column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// get
	@Test
	public void testDoublePrecisionGetBytes() {
		String dml = null;

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertNotNull(rs.getBytes(2));
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}
}