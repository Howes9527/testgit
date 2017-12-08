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

public class BinaryStreamTest extends BaseTest {
	private InputStream[] testBinaryStreamArray = null;
	private String[] testBinaryStreamStringArray = null;

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
	public void testDoublePrecisionSetBinaryStreamByUsingInsertApproachZero() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBinaryStreamStringArray = new String[] { approachZero, minusApproachZero };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStream(dml, testBinaryStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStream insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingInsertApproachZeroWithIntLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBinaryStreamStringArray = new String[] { approachZero, minusApproachZero };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithIntLength(dml, testBinaryStreamArray, false, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithIntLength insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingInsertApproachZeroWithLongLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBinaryStreamStringArray = new String[] { approachZero, minusApproachZero };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithLongLength(dml, testBinaryStreamArray, false, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithLongLength insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingInsert() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBinaryStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStream(dml, testBinaryStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStream insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingInsertWithIntLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBinaryStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithIntLength(dml, testBinaryStreamArray, false, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithIntLength insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingInsertWithLongLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testBinaryStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithLongLength(dml, testBinaryStreamArray, false, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithLongLength insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update
	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpdateApproachZero() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBinaryStreamStringArray = new String[] { approachZero, minusApproachZero };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStream(updateDml, testBinaryStreamArray, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStream update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpdateApproachZeroWithIntLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBinaryStreamStringArray = new String[] { approachZero, minusApproachZero };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithIntLength(updateDml, testBinaryStreamArray, true, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithIntLength update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpdateApproachZeroWithLongLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBinaryStreamStringArray = new String[] { approachZero, minusApproachZero };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithLongLength(updateDml, testBinaryStreamArray, true, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithLongLength update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpdate() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBinaryStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStream(updateDml, testBinaryStreamArray, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStream udpate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpdateWithIntLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBinaryStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithIntLength(updateDml, testBinaryStreamArray, true, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithIntLength update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpdateWithLongLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBinaryStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithLongLength(updateDml, testBinaryStreamArray, true, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithLongLength update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert
	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpsertInsertApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testBinaryStreamStringArray = new String[] { approachZero, minusApproachZero };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStream(upsertDml, testBinaryStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStream upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpsertInsertApproachZeroWithIntLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testBinaryStreamStringArray = new String[] { approachZero, minusApproachZero };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithIntLength(upsertDml, testBinaryStreamArray, false, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithIntLength upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpsertInsertApproachZeroWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testBinaryStreamStringArray = new String[] { approachZero, minusApproachZero };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithLongLength(upsertDml, testBinaryStreamArray, false, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithLongLength upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpsertInsert() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testBinaryStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStream(upsertDml, testBinaryStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStream upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpsertInsertWithIntLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testBinaryStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithIntLength(upsertDml, testBinaryStreamArray, false, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithIntLength upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpsertInsertWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testBinaryStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithLongLength(upsertDml, testBinaryStreamArray, false, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithLongLength upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update
	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpsertUpdateApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBinaryStreamStringArray = new String[] { approachZero, minusApproachZero };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStream(upsertDml, testBinaryStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStream upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpsertUpdateApproachZeroWithIntLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBinaryStreamStringArray = new String[] { approachZero, minusApproachZero };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithIntLength(upsertDml, testBinaryStreamArray, false, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithIntLength upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpsertUpdateApproachZeroWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBinaryStreamStringArray = new String[] { approachZero, minusApproachZero };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithLongLength(upsertDml, testBinaryStreamArray, false, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithLongLength upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpsertUpdate() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBinaryStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStream(upsertDml, testBinaryStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStream udpate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpsertUpdateWithIntLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBinaryStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithIntLength(upsertDml, testBinaryStreamArray, false, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithIntLength upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetBinaryStreamByUsingUpsertUpdateWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testBinaryStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testBinaryStreamArray = new ByteArrayInputStream[] {
					new ByteArrayInputStream((testBinaryStreamStringArray[0]).getBytes()),
					new ByteArrayInputStream((testBinaryStreamStringArray[1]).getBytes()) };
			try {
				insertBinaryStreamWithLongLength(upsertDml, testBinaryStreamArray, false, testBinaryStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setBinaryStreamWithLongLength upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// get
	@Test
	public void testDoublePrecisionGetBinaryStream() {
		String dml = null;

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			InputStream is = new DataInputStream(new ByteArrayInputStream(rs.getString(2).getBytes()));
			BufferedReader bufReader_expect = new BufferedReader(new InputStreamReader(is));
			BufferedReader bufReader_actual = new BufferedReader(new InputStreamReader(rs.getBinaryStream(2)));
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