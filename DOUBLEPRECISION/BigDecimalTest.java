package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class BigDecimalTest extends BaseTest {
	BigDecimal[] bigDecimal = null;
	String[] testBigDecimalStringArray = null;

	BigDecimal testBigDecimal = null;
	String testBigDecimalString = null;

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
	public void testDoubleSetBigDecimalByUsingInsertApproachZero() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBigDecimalStringArray = new String[] { approachZero, minusApproachZero };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]),
					new BigDecimal(testBigDecimalStringArray[1]) };
			insertBigDecimal(dml, bigDecimal, false);

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingInsert() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBigDecimalStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]),
					new BigDecimal(testBigDecimalStringArray[1]) };
			insertBigDecimal(dml, bigDecimal, false);

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update
	@Test
	public void testDoubleSetBigDecimalByUsingUpdateApproachZero() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBigDecimalStringArray = new String[] { approachZero, minusApproachZero };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]),
					new BigDecimal(testBigDecimalStringArray[1]) };
			insertBigDecimal(updateDml, bigDecimal, true);

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingUpdate() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBigDecimalStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]),
					new BigDecimal(testBigDecimalStringArray[1]) };
			insertBigDecimal(updateDml, bigDecimal, true);

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert
	@Test
	public void testDoubleSetBigDecimalByUsingUpsertInsertApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testBigDecimalStringArray = new String[] { approachZero, minusApproachZero };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]),
					new BigDecimal(testBigDecimalStringArray[1]) };
			insertBigDecimal(upsertDml, bigDecimal, false);

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingUpsertInsert() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testBigDecimalStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]),
					new BigDecimal(testBigDecimalStringArray[1]) };
			insertBigDecimal(upsertDml, bigDecimal, false);

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update
	@Test
	public void testDoubleSetBigDecimalByUsingUpsertUpdateApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBigDecimalStringArray = new String[] { approachZero, minusApproachZero };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]),
					new BigDecimal(testBigDecimalStringArray[1]) };
			insertBigDecimal(upsertDml, bigDecimal, false);

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingUpsertUpdate() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBigDecimalStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]),
					new BigDecimal(testBigDecimalStringArray[1]) };
			insertBigDecimal(upsertDml, bigDecimal, false);

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// negative
	@Test
	public void testDoubleSetBigDecimalByUsingInsertApproachZero_negative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBigDecimalStringArray = new String[] { approachZero_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(dml, bigDecimal, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("insert approachZero_negative value to DOUBLEPRECISION column,expected insert failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingInsertApproachZero_Minusnegative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBigDecimalStringArray = new String[] { minusApproachZero_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(dml, bigDecimal, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("insert minusApproachZero_negative value to DOUBLEPRECISION column,expected insert failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingInsert_negative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBigDecimalStringArray = new String[] { DOUBLE_MAX_VALUE_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(dml, bigDecimal, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("insert DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected insert failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingInsert_Minusnegative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBigDecimalStringArray = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(dml, bigDecimal, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("insert MINUS_DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected insert failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update
	@Test
	public void testDoubleSetBigDecimalByUsingUpdateApproachZero_negative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testBigDecimalStringArray = new String[] { approachZero_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(updateDml, bigDecimal, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("update approachZero_negative value to DOUBLEPRECISION column,expected update failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingUpdateApproachZero_Minusnegative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testBigDecimalStringArray = new String[] { minusApproachZero_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(updateDml, bigDecimal, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("update minusApproachZero_negative value to DOUBLEPRECISION column,expected update failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingUpdate_negative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testBigDecimalStringArray = new String[] { DOUBLE_MAX_VALUE_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(updateDml, bigDecimal, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("update DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected update failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingUpdate_MinusNegative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testBigDecimalStringArray = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(updateDml, bigDecimal, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("update MINUS_DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected update failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert
	@Test
	public void testDoubleSetBigDecimalByUsingUpsertInsertApproachZero_negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testBigDecimalStringArray = new String[] { approachZero_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(upsertDml, bigDecimal, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("upsertinsert approachZero_negative value to DOUBLEPRECISION column,expected upsertinsert failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingUpsertInsertApproachZero_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testBigDecimalStringArray = new String[] { minusApproachZero_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(upsertDml, bigDecimal, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("upsertinsert minusApproachZero_negative value to DOUBLEPRECISION column,expected upsertinsert failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingUpsertInsert_negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testBigDecimalStringArray = new String[] { DOUBLE_MAX_VALUE_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(upsertDml, bigDecimal, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("upsertinsert DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected upsertinsert failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingUpsertInsert_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testBigDecimalStringArray = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(upsertDml, bigDecimal, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("upsertinsert MINUS_DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected upsertinsert failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update
	@Test
	public void testDoubleSetBigDecimalByUsingUpsertUpdateApproachZero_negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testBigDecimalStringArray = new String[] { approachZero_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(upsertDml, bigDecimal, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("upsertupdate approachZero_negative value to DOUBLEPRECISION column,expected upsertudpate failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingUpsertUpdateApproachZero_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testBigDecimalStringArray = new String[] { minusApproachZero_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(upsertDml, bigDecimal, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("upsertupdate minusApproachZero_negative value to DOUBLEPRECISION column,expected upsertudpate failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingUpsertUpdate_negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testBigDecimalStringArray = new String[] { DOUBLE_MAX_VALUE_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(upsertDml, bigDecimal, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("upsertupdate DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected upsertudpate failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetBigDecimalByUsingUpsertUpdate_Minusnegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testBigDecimalStringArray = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			bigDecimal = new BigDecimal[] { new BigDecimal(testBigDecimalStringArray[0]) };
			try {
				insertBigDecimal(upsertDml, bigDecimal, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkGetBigDecimal(stmt, testDoubleTable, testBigDecimalStringArray);
			fail("upsertupdate MINUS_DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected upsertudpate failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// get
	@Test
	public void testDoubleByGetBigDecimal() {
		String dml = null;

		try {
			stmt = conn.createStatement();
			dml = "insert into " + schema + "." + testDoubleTable + " values(1, 123456.12345)";
			stmt.executeUpdate(dml);

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(new BigDecimal("123456.12345").doubleValue() == rs.getBigDecimal(2).doubleValue());
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

}
