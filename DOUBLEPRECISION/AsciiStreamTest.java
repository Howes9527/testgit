package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class AsciiStreamTest extends BaseTest {
	private InputStream[] testAsciiStreamArray = null;
	private String[] testAsciiStreamStringArray = null;

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
	public void testDoublePrecisionSetAsciiStreamByUsingInsertApproachZero() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testAsciiStreamStringArray = new String[] { approachZero, minusApproachZero };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStream(dml, testAsciiStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStream insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingInsertApproachZeroWithIntLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testAsciiStreamStringArray = new String[] { approachZero, minusApproachZero };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithIntLength(dml, testAsciiStreamArray, false, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithIntLength insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingInsertApproachZeroWithLongLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testAsciiStreamStringArray = new String[] { approachZero, minusApproachZero };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithLongLength(dml, testAsciiStreamArray, false, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithLongLength insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingInsert() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testAsciiStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStream(dml, testAsciiStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStream insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingInsertWithIntLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testAsciiStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithIntLength(dml, testAsciiStreamArray, false, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithIntLength insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingInsertWithLongLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testAsciiStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithLongLength(dml, testAsciiStreamArray, false, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithLongLength insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update
	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpdateApproachZero() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();

			testAsciiStreamStringArray = new String[] { approachZero, minusApproachZero };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStream(updateDml, testAsciiStreamArray, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStream update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpdateApproachZeroWithIntLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testAsciiStreamStringArray = new String[] { approachZero, minusApproachZero };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithIntLength(updateDml, testAsciiStreamArray, true, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithIntLength update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpdateApproachZeroWithLongLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testAsciiStreamStringArray = new String[] { approachZero, minusApproachZero };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithLongLength(updateDml, testAsciiStreamArray, true, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithLongLength update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpdate() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testAsciiStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStream(updateDml, testAsciiStreamArray, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStream udpate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpdateWithIntLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testAsciiStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithIntLength(updateDml, testAsciiStreamArray, true, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithIntLength update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpdateWithLongLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testAsciiStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithLongLength(updateDml, testAsciiStreamArray, true, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithLongLength update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert
	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpsertInsertApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testAsciiStreamStringArray = new String[] { approachZero, minusApproachZero };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStream(upsertDml, testAsciiStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStream upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpsertInsertApproachZeroWithIntLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testAsciiStreamStringArray = new String[] { approachZero, minusApproachZero };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithIntLength(upsertDml, testAsciiStreamArray, false, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithIntLength upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpsertInsertApproachZeroWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testAsciiStreamStringArray = new String[] { approachZero, minusApproachZero };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithLongLength(upsertDml, testAsciiStreamArray, false, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithLongLength upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpsertInsert() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testAsciiStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStream(upsertDml, testAsciiStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStream upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpsertInsertWithIntLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testAsciiStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithIntLength(upsertDml, testAsciiStreamArray, false, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithIntLength upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpsertInsertWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testAsciiStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithLongLength(upsertDml, testAsciiStreamArray, false, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithLongLength upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update
	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpsertUpdateApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testAsciiStreamStringArray = new String[] { approachZero, minusApproachZero };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStream(upsertDml, testAsciiStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStream upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpsertUpdateApproachZeroWithIntLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testAsciiStreamStringArray = new String[] { approachZero, minusApproachZero };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithIntLength(upsertDml, testAsciiStreamArray, false, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithIntLength upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpsertUpdateApproachZeroWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testAsciiStreamStringArray = new String[] { approachZero, minusApproachZero };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithLongLength(upsertDml, testAsciiStreamArray, false, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithLongLength upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpsertUpdate() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testAsciiStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStream(upsertDml, testAsciiStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStream udpate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpsertUpdateWithIntLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testAsciiStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithIntLength(upsertDml, testAsciiStreamArray, false, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithIntLength upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetAsciiStreamByUsingUpsertUpdateWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testAsciiStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testAsciiStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testAsciiStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testAsciiStreamStringArray[1]).getBytes()) };
			try {
				insertAsciiStreamWithLongLength(upsertDml, testAsciiStreamArray, false, testAsciiStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setAsciiStreamWithLongLength upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// get
	@Test
	public void testDoublePrecisionGetAsciiStream() {
		String dml = null;

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			InputStream is = new DataInputStream(new ByteArrayInputStream(rs.getString(2).getBytes()));
			BufferedReader bufReader_expect = new BufferedReader(new InputStreamReader(is));
			BufferedReader bufReader_actual = new BufferedReader(new InputStreamReader(rs.getAsciiStream(2)));
			assertEquals(bufReader_expect.readLine(), bufReader_actual.readLine());
		} catch (SQLException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

}