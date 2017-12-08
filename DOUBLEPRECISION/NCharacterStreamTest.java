package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class NCharacterStreamTest extends BaseTest {
	private StringReader[] testNCharacterStreamArray = null;
	private String[] testNCharacterStreamStringArray = null;

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
	public void testDoublePrecisionSetNCharacterStreamByUsingInsertApproachZero() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testNCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStream(dml, testNCharacterStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStream insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetNCharacterStreamByUsingInsertApproachZeroWithLongLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testNCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStreamWithLongLength(dml, testNCharacterStreamArray, false,
						testNCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStreamWithLongLength insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetNCharacterStreamByUsingInsert() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testNCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStream(dml, testNCharacterStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStream insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetNCharacterStreamByUsingInsertWithLongLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testNCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStreamWithLongLength(dml, testNCharacterStreamArray, false,
						testNCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStreamWithLongLength insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update
	@Test
	public void testDoublePrecisionSetNCharacterStreamByUsingUpdateApproachZero() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testNCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStream(updateDml, testNCharacterStreamArray, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStream update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetNCharacterStreamByUsingUpdateApproachZeroWithLongLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testNCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStreamWithLongLength(updateDml, testNCharacterStreamArray, true,
						testNCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStreamWithLongLength update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetNCharacterStreamByUsingUpdate() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testNCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStream(updateDml, testNCharacterStreamArray, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStream udpate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetNCharacterStreamByUsingUpdateWithLongLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testNCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStreamWithLongLength(updateDml, testNCharacterStreamArray, true,
						testNCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStreamWithLongLength update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert
	@Test
	public void testDoublePrecisionSetNCharacterStreamByUsingUpsertInsertApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testNCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStream(upsertDml, testNCharacterStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStream upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetNCharacterStreamByUsingUpsertInsertApproachZeroWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testNCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStreamWithLongLength(upsertDml, testNCharacterStreamArray, false,
						testNCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStreamWithLongLength upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetNCharacterStreamByUsingUpsertInsert() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testNCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStream(upsertDml, testNCharacterStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStream upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetNCharacterStreamByUsingUpsertInsertWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testNCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStreamWithLongLength(upsertDml, testNCharacterStreamArray, false,
						testNCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStreamWithLongLength upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update
	@Test
	public void testDoublePrecisionSetNCharacterStreamByUsingUpsertUpdateApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testNCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStream(upsertDml, testNCharacterStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStream upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetNCharacterStreamByUsingUpsertUpdateApproachZeroWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testNCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStreamWithLongLength(upsertDml, testNCharacterStreamArray, false,
						testNCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStreamWithLongLength upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetNCharacterStreamByUsingUpsertUpdate() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testNCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStream(upsertDml, testNCharacterStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStream udpate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetNCharacterStreamByUsingUpsertUpdateWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testNCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testNCharacterStreamArray = new StringReader[] { new StringReader((testNCharacterStreamStringArray[0])),
					new StringReader((testNCharacterStreamStringArray[1])) };
			try {
				insertNCharacterStreamWithLongLength(upsertDml, testNCharacterStreamArray, false,
						testNCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setNCharacterStreamWithLongLength upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// get
	@Test
	public void testDoublePrecisionGetNCharacterStream() {
		String dml = null;

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			Reader reader = rs.getNCharacterStream(2);
			String restult = new BufferedReader(reader).readLine();
			assertTrue(Double.parseDouble("1.0") == Double.parseDouble(restult.trim()));
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
