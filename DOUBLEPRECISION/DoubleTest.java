package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class DoubleTest extends BaseTest {
	private Double[] testDouble = null;
	private String[] testDoubleString = null;

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

	// insert
	@Test
	public void testDoubleSetDoubleByUsingInsertApproachZero() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testDoubleString = new String[] { approachZero, minusApproachZero };
			testDouble = new Double[] { new Double(testDoubleString[0]), new Double(testDoubleString[1]) };
			insertDouble(dml, testDouble, false);

			checkDouble(stmt, testDoubleTable, testDoubleString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingInsert() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testDoubleString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testDouble = new Double[] { new Double(testDoubleString[0]), new Double(testDoubleString[1]) };
			insertDouble(dml, testDouble, false);

			checkDouble(stmt, testDoubleTable, testDoubleString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// insert negative
	@Test
	public void testDoubleSetDoubleByUsingInsertApproachZero_Negative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testDoubleString = new String[] { approachZero_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(dml, testDouble, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("insert approachZero_negative value to DOUBLEPRECISION column,expected insert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingInsertApproachZero_MinusNegative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testDoubleString = new String[] { minusApproachZero_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(dml, testDouble, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("insert minusApproachZero_negative value to DOUBLEPRECISION column,expected insert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingInsert_Negative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testDoubleString = new String[] { DOUBLE_MAX_VALUE_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(dml, testDouble, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("insert DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected insert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingInsert_MinusNegative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testDoubleString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(dml, testDouble, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("insert MINUS_DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected insert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update
	@Test
	public void testDoubleSetDoubleByUsingUpdateApproachZero() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testDoubleString = new String[] { approachZero, minusApproachZero };
			testDouble = new Double[] { new Double(testDoubleString[0]), new Double(testDoubleString[1]) };
			insertDouble(updateDml, testDouble, true);

			checkDouble(stmt, testDoubleTable, testDoubleString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingUpdate() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testDoubleString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testDouble = new Double[] { new Double(testDoubleString[0]), new Double(testDoubleString[1]) };
			insertDouble(updateDml, testDouble, true);

			checkDouble(stmt, testDoubleTable, testDoubleString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update negative
	@Test
	public void testDoubleSetDoubleByUsingUpdateApproachZero_Negative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testDoubleString = new String[] { approachZero_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(updateDml, testDouble, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("update approachZero_negative value to DOUBLEPRECISION column,expected update failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingUpdateApproachZero_MinusNegative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testDoubleString = new String[] { minusApproachZero_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(updateDml, testDouble, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("update minusApproachZero_negative value to DOUBLEPRECISION column,expected update failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingUpdate_Negative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testDoubleString = new String[] { DOUBLE_MAX_VALUE_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(updateDml, testDouble, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("update DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected update failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingUpdate_MinusNegative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testDoubleString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(updateDml, testDouble, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("update MINUS_DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected update failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert
	@Test
	public void testDoubleSetDoubleByUsingUpsertInsertApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testDoubleString = new String[] { approachZero, minusApproachZero };
			testDouble = new Double[] { new Double(testDoubleString[0]), new Double(testDoubleString[1]) };
			insertDouble(upsertDml, testDouble, false);

			checkDouble(stmt, testDoubleTable, testDoubleString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingUpsertInsert() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testDoubleString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testDouble = new Double[] { new Double(testDoubleString[0]), new Double(testDoubleString[1]) };
			insertDouble(upsertDml, testDouble, false);

			checkDouble(stmt, testDoubleTable, testDoubleString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert negative
	@Test
	public void testDoubleSetDoubleByUsingUpsertInsertApproachZero_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testDoubleString = new String[] { approachZero_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(upsertDml, testDouble, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("upsertinsert approachZero_negative value to DOUBLEPRECISION column,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingUpsertInsertApproachZero_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testDoubleString = new String[] { minusApproachZero_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(upsertDml, testDouble, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("upsertinsert minusApproachZero_negative value to DOUBLEPRECISION column,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingUpsertInsert_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testDoubleString = new String[] { DOUBLE_MAX_VALUE_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(upsertDml, testDouble, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("upsertinsert DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingUpsertInsert_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testDoubleString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(upsertDml, testDouble, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("upsertinsert MINUS_DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update
	@Test
	public void testDoubleSetDoubleByUsingUpsertUpdateApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testDoubleString = new String[] { approachZero, minusApproachZero };
			testDouble = new Double[] { new Double(testDoubleString[0]), new Double(testDoubleString[1]) };
			insertDouble(upsertDml, testDouble, false);

			checkDouble(stmt, testDoubleTable, testDoubleString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingUpsertUpdate() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testDoubleString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testDouble = new Double[] { new Double(testDoubleString[0]), new Double(testDoubleString[1]) };
			insertDouble(upsertDml, testDouble, false);

			checkDouble(stmt, testDoubleTable, testDoubleString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update negative
	@Test
	public void testDoubleSetDoubleByUsingUpsertUpdateApproachZero_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testDoubleString = new String[] { approachZero_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(upsertDml, testDouble, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("upsertupdate approachZero_negative value to DOUBLEPRECISION column,expected upsertupdate failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingUpsertUpdateApproachZero_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testDoubleString = new String[] { minusApproachZero_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(upsertDml, testDouble, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("upsertupdate minusApproachZero_negative value to DOUBLEPRECISION column,expected upsertupdate failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingUpsertUpdate_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testDoubleString = new String[] { DOUBLE_MAX_VALUE_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(upsertDml, testDouble, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("upsertupdate DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected upsertupdate failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetDoubleByUsingUpsertUpdate_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testDoubleString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testDouble = new Double[] { new Double(testDoubleString[0]) };
			try {
				insertDouble(upsertDml, testDouble, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkDouble(stmt, testDoubleTable, testDoubleString);
			fail("upsertupdate MINUS_DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected upsertupdate failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// get
	@Test
	public void testDoubleByGetDouble() {
		String dml = null;

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(Double.parseDouble("1.0") == rs.getDouble(2));
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}
}
