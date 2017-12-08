package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class NStringTest extends BaseTest {
	private String[] testNString = null;

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
	public void testDoubleSetNStringByUsingInsertApproachZero() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testNString = new String[] { approachZero, minusApproachZero };
			insertNString(dml, testNString, false);

			checkNString(stmt, testDoubleTable, testNString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetNStringByUsingInsert() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testNString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			insertNString(dml, testNString, false);

			checkNString(stmt, testDoubleTable, testNString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update
	@Test
	public void testDoubleSetNStringByUsingUpdateApproachZero() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testNString = new String[] { approachZero, minusApproachZero };
			insertNString(updateDml, testNString, true);

			checkNString(stmt, testDoubleTable, testNString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetNStringByUsingUpdate() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testNString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			insertNString(updateDml, testNString, true);

			checkNString(stmt, testDoubleTable, testNString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert
	@Test
	public void testDoubleSetNStringByUsingUpsertInsertApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testNString = new String[] { approachZero, minusApproachZero };
			insertNString(upsertDml, testNString, false);

			checkNString(stmt, testDoubleTable, testNString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetNStringByUsingUpsertInsert() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testNString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			insertNString(upsertDml, testNString, false);

			checkNString(stmt, testDoubleTable, testNString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update
	@Test
	public void testDoubleSetNStringByUsingUpsertUpdateApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testNString = new String[] { approachZero, minusApproachZero };
			insertNString(upsertDml, testNString, false);

			checkNString(stmt, testDoubleTable, testNString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetNStringByUsingUpsertUpdate() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testNString = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			insertNString(upsertDml, testNString, false);

			checkNString(stmt, testDoubleTable, testNString);
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// negative
	@Test
	public void testDoubleSetNStringByUsingInsertapproachZero_negative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testNString = new String[] { approachZero_negative };

			try {
				insertNString(dml, testNString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
			fail("insert approachZero_negative value to DOUBLEPRECISION column,expected insert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetNStringByUsingInsert_negative() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testNString = new String[] { DOUBLE_MAX_VALUE_negative };

			try {
				insertNString(dml, testNString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
			fail("insert DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected insert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update
	@Test
	public void testDoubleSetNStringByUsingUpdateapproachZero_negative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testNString = new String[] { approachZero_negative };

			try {
				insertNString(updateDml, testNString, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
			fail("update approachZero_negative value to DOUBLEPRECISION column,expected update failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetNStringByUsingUpdate_negative() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testNString = new String[] { DOUBLE_MAX_VALUE_negative };

			try {
				insertNString(updateDml, testNString, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
			fail("update DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected update failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert
	@Test
	public void testDoubleSetNStringByUsingUpsertInsertapproachZero_negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testNString = new String[] { approachZero_negative };

			try {
				insertNString(upsertDml, testNString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
			fail("upsertinsert approachZero_negative value to DOUBLEPRECISION column,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetNStringByUsingUpsertInsert_negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testNString = new String[] { DOUBLE_MAX_VALUE_negative };

			try {
				insertNString(upsertDml, testNString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
			fail("upsertinsert DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update
	@Test
	public void testDoubleSetNStringByUsingUpsertUpdateapproachZero_negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testNString = new String[] { approachZero_negative };

			try {
				insertNString(upsertDml, testNString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
			fail("upsertupdate approachZero_negative value to DOUBLEPRECISION column,expected upsertupdate failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetNStringByUsingUpsertUpdate_negative() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testNString = new String[] { DOUBLE_MAX_VALUE_negative };

			try {
				insertNString(upsertDml, testNString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
			fail("upsertupdate DOUBLE_MAX_VALUE_negative value to DOUBLEPRECISION column,expected upsertupdate failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// minus negative
	@Test
	public void testDoubleSetNStringByUsingInsertminusApproachZero_negativeMinus() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testNString = new String[] { minusApproachZero_negative };

			try {
				insertNString(dml, testNString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
			fail("insert minusApproachZero_negative value to DOUBLEPRECISION column,expected insert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetNStringByUsingInsert_negativeMinus() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testNString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };

			try {
				insertNString(dml, testNString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
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
	public void testDoubleSetNStringByUsingUpdateminusApproachZero_negativeMinus() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testNString = new String[] { minusApproachZero_negative };

			try {
				insertNString(updateDml, testNString, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
			fail("update minusApproachZero_negative value to DOUBLEPRECISION column,expected update failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetNStringByUsingUpdate_negativeMinus() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testNString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };

			try {
				insertNString(updateDml, testNString, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
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
	public void testDoubleSetNStringByUsingUpsertInsertminusApproachZero_negativeMinus() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testNString = new String[] { minusApproachZero_negative };

			try {
				insertNString(upsertDml, testNString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
			fail("upsertinsert minusApproachZero_negative value to DOUBLEPRECISION column,expected upsertinsert failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetNStringByUsingUpsertInsert_negativeMinus() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testNString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };

			try {
				insertNString(upsertDml, testNString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
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
	public void testDoubleSetNStringByUsingUpsertUpdateminusApproachZero_negativeMinus() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testNString = new String[] { minusApproachZero_negative };

			try {
				insertNString(upsertDml, testNString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
			fail("upsertupdate minusApproachZero_negative value to DOUBLEPRECISION column,expected upsertupdate failed,but successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoubleSetNStringByUsingUpsertUpdate_negativeMinus() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			testNString = new String[] { MINUS_DOUBLE_MAX_VALUE_negative };

			try {
				insertNString(upsertDml, testNString, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("out of range") || e.getMessage().contains("overflow")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			checkNString(stmt, testDoubleTable, testNString);
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
	public void testDoubleByGetNString() {
		String dml = null;

		try {
			stmt = conn.createStatement();
			dml = "insert into " + schema + "." + testDoubleTable + " values(1, 88.7)";
			stmt.executeUpdate(dml);

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertEquals("88.7", rs.getNString(2).trim());
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}
}
