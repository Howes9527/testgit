package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class ObjectTest extends BaseTest {
	private Object[] testObject = null;
	private String[] testObjectString = null;

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
	public void testDoubleSetObjectByUsingInsertApproachZero() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { approachZero, minusApproachZero };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObject(dml, testObject, false);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingInsert() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObject(dml, testObject, false);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// insert negative
	@Test
	public void testDoubleSetObjectByUsingInsertApproachZero_Negative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { approachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(dml, testObject, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("insert approachZeroNegative value to DOUBLEPRECISION cloumn,expected insert fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingInsertApproachZero_MinusNegative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { minusApproachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(dml, testObject, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("insert minusApproachZeroNegative value to DOUBLEPRECISION cloumn,expected insert fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingInsert_Negative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(dml, testObject, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("insert approachZeroNegative value to DOUBLEPRECISION cloumn,expected insert fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingInsert_MinusNegative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(dml, testObject, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("insert minusApproachZeroNegative value to DOUBLEPRECISION cloumn,expected insert fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// insert with sqltype
	@Test
	public void testDoubleSetObjectByUsingInsertApproachZeroWithSQLType() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { approachZero, minusApproachZero };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithSQLType(dml, testObject, false, java.sql.Types.DOUBLE);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingInsertWithSQLType() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithSQLType(dml, testObject, false, java.sql.Types.DOUBLE);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// insert with sqltype negative
	@Test
	public void testDoubleSetObjectByUsingInsertApproachZeroWithSQLType_Negative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { approachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(dml, testObject, false, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("insert approachZeroNegative WithSQLType value to DOUBLEPRECISION cloumn,expected insert fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingInsertApproachZeroWithSQLType_MinusNegative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { minusApproachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(dml, testObject, false, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("insert minusApproachZeroNegative WithSQLType value to DOUBLEPRECISION cloumn,expected insert fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingInsertWithSQLType_Negative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(dml, testObject, false, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("insert approachZeroNegative WithSQLType value to DOUBLEPRECISION cloumn,expected insert fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingInsertWithSQLType_MinusNegative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(dml, testObject, false, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("insert minusApproachZeroNegative WithSQLType value to DOUBLEPRECISION cloumn,expected insert fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// insert with scalaLength
	@Test
	public void testDoubleSetObjectByUsingInsertApproachZeroWithScalaLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { approachZero, minusApproachZero };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithScalaLength(dml, testObject, false, java.sql.Types.DOUBLE, 0);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingInsertWithScalaLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithScalaLength(dml, testObject, false, java.sql.Types.DOUBLE, 0);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// insert with scalaLength negative
	@Test
	public void testDoubleSetObjectByUsingInsertApproachZeroWithScalaLength_Negative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { approachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(dml, testObject, false, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("insert approachZeroNegative WithScalaLength value to DOUBLEPRECISION cloumn,expected insert fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingInsertApproachZeroWithScalaLength_MinusNegative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { minusApproachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(dml, testObject, false, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("insert minusApproachZeroNegative WithScalaLength value to DOUBLEPRECISION cloumn,expected insert fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingInsertWithScalaLength_Negative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(dml, testObject, false, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("insert approachZeroNegative WithScalaLength value to DOUBLEPRECISION cloumn,expected insert fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingInsertWithScalaLength_MinusNegative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(dml, testObject, false, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("insert minusApproachZeroNegative WithScalaLength value to DOUBLEPRECISION cloumn,expected insert fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update
	@Test
	public void testDoubleSetObjectByUsingUpdateApproachZero() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { approachZero, minusApproachZero };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObject(updateDml, testObject, true);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpdate() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObject(updateDml, testObject, true);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update negative
	@Test
	public void testDoubleSetObjectByUsingUpdateApproachZero_Negative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { approachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(updateDml, testObject, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("update approachZeroNegative value to DOUBLEPRECISION column,expected update fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpdateApproachZero_MinusNegative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { minusApproachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(updateDml, testObject, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("update approachZeroNegative value to DOUBLEPRECISION column,expected update fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpdate_Negative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(updateDml, testObject, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("update approachZeroNegative value to DOUBLEPRECISION column,expected update fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpdate_MinusNegative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(updateDml, testObject, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("update approachZeroNegative value to DOUBLEPRECISION column,expected update fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update with sqltype
	@Test
	public void testDoubleSetObjectByUsingUpdateApproachZeroWithSQLType() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { approachZero, minusApproachZero };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithSQLType(updateDml, testObject, true, java.sql.Types.DOUBLE);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpdateWithSQLType() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithSQLType(updateDml, testObject, true, java.sql.Types.DOUBLE);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update with sqltype negative
	@Test
	public void testDoubleSetObjectByUsingUpdateApproachZeroWithSQLType_Negstive() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { approachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(updateDml, testObject, true, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("update approachZeroNegative WithSQLType value to DOUBLEPRECISION column,expected update fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpdateApproachZeroWithSQLType_MinusNegstive() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { minusApproachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(updateDml, testObject, true, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("update approachZeroNegative WithSQLType value to DOUBLEPRECISION column,expected update fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpdateWithSQLType_Negstive() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(updateDml, testObject, true, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("update approachZeroNegative WithSQLType value to DOUBLEPRECISION column,expected update fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpdateWithSQLType_MinusNegstive() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(updateDml, testObject, true, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("update approachZeroNegative WithSQLType value to DOUBLEPRECISION column,expected update fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update with scalaLength
	@Test
	public void testDoubleSetObjectByUsingUpdateApproachZeroWithScalaLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { approachZero, minusApproachZero };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithScalaLength(updateDml, testObject, true, java.sql.Types.DOUBLE, 0);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpdateWithScalaLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithScalaLength(updateDml, testObject, true, java.sql.Types.DOUBLE, 0);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update with scalaLength negative
	@Test
	public void testDoubleSetObjectByUsingUpdateApproachZeroWithScalaLength_Negstive() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { approachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(updateDml, testObject, true, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("update approachZeroNegative WithScalaLength value to DOUBLEPRECISION column,expected update fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpdateApproachZeroWithScalaLength_MinusNegstive() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { minusApproachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(updateDml, testObject, true, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("update approachZeroNegative WithScalaLength value to DOUBLEPRECISION column,expected update fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpdateWithScalaLength_Negative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(updateDml, testObject, true, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("update approachZeroNegative WithScalaLength value to DOUBLEPRECISION column,expected update fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpdateWithScalaLength_MinusNegative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(updateDml, testObject, true, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("update approachZeroNegative WithScalaLength value to DOUBLEPRECISION column,expected update fialed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert
	@Test
	public void testDoubleSetObjectByUsingUpsertInsertApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { approachZero, minusApproachZero };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObject(upsertDml, testObject, false);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertInsert() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObject(upsertDml, testObject, false);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert negative
	@Test
	public void testDoubleSetObjectByUsingUpsertInsertApproachZero_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { approachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(upsertDml, testObject, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertinsert approachZeroNegative value to DOUBLEPRECISION cloumn,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertInsertApproachZero_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { minusApproachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(upsertDml, testObject, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertinsert minusApproachZeroNegative value to DOUBLEPRECISION cloumn,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertInsert_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(upsertDml, testObject, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertinsert approachZeroNegative value to DOUBLEPRECISION cloumn,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertInsert_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(upsertDml, testObject, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertinsert minusApproachZeroNegative value to DOUBLEPRECISION cloumn,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert with sqltype
	@Test
	public void testDoubleSetObjectByUsingUpsertInsertApproachZeroWithSQLType() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { approachZero, minusApproachZero };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithSQLType(upsertDml, testObject, false, java.sql.Types.DOUBLE);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertInsertWithSQLType() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithSQLType(upsertDml, testObject, false, java.sql.Types.DOUBLE);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert with sqltype negative
	@Test
	public void testDoubleSetObjectByUsingUpsertInsertApproachZeroWithSQLType_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { approachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(upsertDml, testObject, false, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertinsert approachZeroNegative WithSQLType value to DOUBLEPRECISION cloumn,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertInsertApproachZeroWithSQLType_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { minusApproachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(upsertDml, testObject, false, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertinsert minusApproachZeroNegative WithSQLType value to DOUBLEPRECISION cloumn,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertInsertWithSQLType_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(upsertDml, testObject, false, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertinsert approachZeroNegative WithSQLType value to DOUBLEPRECISION cloumn,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertInsertWithSQLType_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(upsertDml, testObject, false, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertinsert minusApproachZeroNegative WithSQLType value to DOUBLEPRECISION cloumn,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert with scalaLength
	@Test
	public void testDoubleSetObjectByUsingUpsertInsertApproachZeroWithScalaLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { approachZero, minusApproachZero };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithScalaLength(upsertDml, testObject, false, java.sql.Types.DOUBLE, 0);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertInsertWithScalaLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithScalaLength(upsertDml, testObject, false, java.sql.Types.DOUBLE, 0);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert with scalaLength negative
	@Test
	public void testDoubleSetObjectByUsingUpsertInsertApproachZeroWithScalaLength_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { approachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(upsertDml, testObject, false, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertinsert approachZeroNegative WithScalaLength value to DOUBLEPRECISION cloumn,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertInsertApproachZeroWithScalaLength_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { minusApproachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(upsertDml, testObject, false, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertinsert minusApproachZeroNegative WithScalaLength value to DOUBLEPRECISION cloumn,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertInsertWithScalaLength_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(upsertDml, testObject, false, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertinsert approachZeroNegative WithScalaLength value to DOUBLEPRECISION cloumn,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertInsertWithScalaLength_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testObjectString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(upsertDml, testObject, false, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertinsert minusApproachZeroNegative WithScalaLength value to DOUBLEPRECISION cloumn,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update
	@Test
	public void testDoubleSetObjectByUsingUpsertUpdateApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { approachZero, minusApproachZero };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObject(upsertDml, testObject, false);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertUpdate() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObject(upsertDml, testObject, false);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update negative
	@Test
	public void testDoubleSetObjectByUsingUpsertUpdateApproachZero_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { approachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(upsertDml, testObject, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertupdate approachZeroNegative value to DOUBLEPRECISION column,expected upsertupdate failed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertUpdateApproachZero_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { minusApproachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(upsertDml, testObject, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertupdate minusApproachZeroNegative value to DOUBLEPRECISION column,expected upsertupdate failed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertUpdate_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(upsertDml, testObject, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertupdate approachZeroNegative value to DOUBLEPRECISION column,expected upsertupdate failed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertUpdate_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObject(upsertDml, testObject, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertupdate minusApproachZeroNegative value to DOUBLEPRECISION column,expected upsertupdate failed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update with sqltype
	@Test
	public void testDoubleSetObjectByUsingUpsertUpdateApproachZeroWithSQLType() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { approachZero, minusApproachZero };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithSQLType(upsertDml, testObject, false, java.sql.Types.DOUBLE);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertUpdateWithSQLType() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithSQLType(upsertDml, testObject, false, java.sql.Types.DOUBLE);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update with sqltype negative
	@Test
	public void testDoubleSetObjectByUsingUpsertUpdateApproachZeroWithSQLType_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { approachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(upsertDml, testObject, false, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertupdate approachZeroNegative WithSQLType value to DOUBLEPRECISION column,expected upsertupdate failed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertUpdateApproachZeroWithSQLType_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { minusApproachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(upsertDml, testObject, false, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertupdate minusApproachZeroNegative WithSQLType value to DOUBLEPRECISION column,expected upsertupdate failed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertUpdateWithSQLType_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(upsertDml, testObject, false, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertupdate approachZeroNegative WithSQLType value to DOUBLEPRECISION column,expected upsertupdate failed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertUpdateWithSQLType_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithSQLType(upsertDml, testObject, false, java.sql.Types.DOUBLE);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertupdate minusApproachZeroNegative WithSQLType value to DOUBLEPRECISION column,expected upsertupdate failed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update with scalaLength
	@Test
	public void testDoubleSetObjectByUsingUpsertUpdateApproachZeroWithScalaLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { approachZero, minusApproachZero };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithScalaLength(upsertDml, testObject, false, java.sql.Types.DOUBLE, 0);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertUpdateWithScalaLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testObject = new Object[] { new Double(testObjectString[0]), new Double(testObjectString[1]) };
			insertObjectWithScalaLength(upsertDml, testObject, false, java.sql.Types.DOUBLE, 0);

			checkObject(stmt, testDoubleTable, testObjectString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update with scalaLdength negative
	@Test
	public void testDoubleSetObjectByUsingUpsertUpdateApproachZeroWithScalaLength_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { approachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(upsertDml, testObject, false, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertupdate approachZeroNegative WithScalaLength value to DOUBLEPRECISION column,expected upsertupdate failed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertUpdateApproachZeroWithScalaLength_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { minusApproachZero_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(upsertDml, testObject, false, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertupdate minusApproachZeroNegative WithScalaLength value to DOUBLEPRECISION column,expected upsertupdate failed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertUpdateWithScalaLength_Negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(upsertDml, testObject, false, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertupdate approachZeroNegative WithScalaLength value to DOUBLEPRECISION column,expected upsertupdate failed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetObjectByUsingUpsertUpdateWithScalaLength_MinusNegative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testObjectString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };
			testObject = new Object[] { new Double(testObjectString[0]) };
			try {
				insertObjectWithScalaLength(upsertDml, testObject, false, java.sql.Types.DOUBLE, 0);
			} catch (SQLException e) {
				if (e.getMessage().contains("Numeric value out of range")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkObject(stmt, testDoubleTable, testObjectString);
			fail("upsertupdate minusApproachZeroNegative WithScalaLength value to DOUBLEPRECISION column,expected upsertupdate failed,but succssfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// get
	@Test
	public void testDoubleByGetObject() {
		String dml = null;

		try {
			stmt = conn.createStatement();
			dml = "insert into " + schema + "." + testDoubleTable + " values(1, 1234.12345)";
			stmt.executeUpdate(dml);

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertEquals(new Double("1234.12345"), rs.getObject(2));
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

}