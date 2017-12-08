package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class StringTest extends BaseTest {
	private String[] testString = null;

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
	public void testDoubleSetStringByUsingInsertApproachZero() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testString = new String[] { approachZero, minusApproachZero };
			insertString(dml, testString, false);

			checkString(stmt, testDoubleTable, testString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetStringByUsingInsert() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			insertString(dml, testString, false);

			checkString(stmt, testDoubleTable, testString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update
	@Test
	public void testDoubleSetStringByUsingUpdateApproachZero() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testString = new String[] { approachZero, minusApproachZero };
			insertString(updateDml, testString, true);

			checkString(stmt, testDoubleTable, testString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetStringByUsingUpdate() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			insertString(updateDml, testString, true);

			checkString(stmt, testDoubleTable, testString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert
	@Test
	public void testDoubleSetStringByUsingUpsertInsertApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testString = new String[] { approachZero, minusApproachZero };
			insertString(upsertDml, testString, false);

			checkString(stmt, testDoubleTable, testString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetStringByUsingUpsertInsert() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			insertString(upsertDml, testString, false);

			checkString(stmt, testDoubleTable, testString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update
	@Test
	public void testDoubleSetStringByUsingUpsertUpdateApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testString = new String[] { approachZero, minusApproachZero };
			insertString(upsertDml, testString, false);

			checkString(stmt, testDoubleTable, testString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetStringByUsingUpsertUpdate() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			insertString(upsertDml, testString, false);

			checkString(stmt, testDoubleTable, testString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// negative
	@Test
	public void testDoubleSetStringByUsingInsertapproachZero_negative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testString = new String[] { approachZero_negative };

			try {
				insertString(dml, testString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
			fail("insert approachZero_negative value to DOUBLEPRECISION column,expected insert failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetStringByUsingInsert_negative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testString = new String[] { DOUBLE_MAX_VALUE_negative };

			try {
				insertString(dml, testString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
			fail("insert DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected insert failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update
	@Test
	public void testDoubleSetStringByUsingUpdateapproachZero_negative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testString = new String[] { approachZero_negative };

			try {
				insertString(updateDml, testString, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
			fail("update approachZero_negative value to DOUBLEPRECISION column,expected update failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetStringByUsingUpdate_negative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testString = new String[] { DOUBLE_MAX_VALUE_negative };

			try {
				insertString(updateDml, testString, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
			fail("update DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected update failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert
	@Test
	public void testDoubleSetStringByUsingUpsertInsertapproachZero_negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testString = new String[] { approachZero_negative };

			try {
				insertString(upsertDml, testString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
			fail("upsertinsert approachZero_negative value to DOUBLEPRECISION column,expected upsertinsert failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetStringByUsingUpsertInsert_negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testString = new String[] { DOUBLE_MAX_VALUE_negative };

			try {
				insertString(upsertDml, testString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
			fail("upsertinsert DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected upsertinsert failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update
	@Test
	public void testDoubleSetStringByUsingUpsertUpdateapproachZero_negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testString = new String[] { approachZero_negative };

			try {
				insertString(upsertDml, testString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
			fail("upsertupdate approachZero_negative value to DOUBLEPRECISION column,expected upsertupdate failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetStringByUsingUpsertUpdate_negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testString = new String[] { DOUBLE_MAX_VALUE_negative };

			try {
				insertString(upsertDml, testString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
			fail("upsertupdate DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected upsertupdate failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// minus negative
	@Test
	public void testDoubleSetStringByUsingInsertminusApproachZero_negativeMinus() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testString = new String[] { minusApproachZero_negative };

			try {
				insertString(dml, testString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
			fail("insert minusApproachZero_negative value to DOUBLEPRECISION column,expected insert failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetStringByUsingInsert_negativeMinus() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };

			try {
				insertString(dml, testString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
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
	public void testDoubleSetStringByUsingUpdateminusApproachZero_negativeMinus() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testString = new String[] { minusApproachZero_negative };

			try {
				insertString(updateDml, testString, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
			fail("update minusApproachZero_negative value to DOUBLEPRECISION column,expected update failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetStringByUsingUpdate_negativeMinus() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };

			try {
				insertString(updateDml, testString, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
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
	public void testDoubleSetStringByUsingUpsertInsertminusApproachZero_negativeMinus() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testString = new String[] { minusApproachZero_negative };

			try {
				insertString(upsertDml, testString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
			fail("upsertinsert minusApproachZero_negative value to DOUBLEPRECISION column,expected upsertinsert failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetStringByUsingUpsertInsert_negativeMinus() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };

			try {
				insertString(upsertDml, testString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
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
	public void testDoubleSetStringByUsingUpsertUpdateminusApproachZero_negativeMinus() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testString = new String[] { minusApproachZero_negative };

			try {
				insertString(upsertDml, testString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
			fail("upsertupdate minusApproachZero_negative value to DOUBLEPRECISION column,expected upsertupdate failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetStringByUsingUpsertUpdate_negativeMinus() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };

			try {
				insertString(upsertDml, testString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkString(stmt, testDoubleTable, testString);
			fail("upsertupdate MINUS_DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected upsertupdate failed,but sccessfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleByGetString() {
		String dml = null;

		try {
			stmt = conn.createStatement();
			dml = "insert into " + schema + "." + testDoubleTable + " values(1, 1234.12345)";
			stmt.executeUpdate(dml);

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertEquals("1234.12345", rs.getString(2).trim());
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}
}
