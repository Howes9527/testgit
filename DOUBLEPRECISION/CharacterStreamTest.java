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

public class CharacterStreamTest extends BaseTest {
	private StringReader[] testCharacterStreamArray = null;
	private String[] testCharacterStreamStringArray = null;

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
	public void testDoublePrecisionSetCharacterStreamByUsingInsertApproachZero() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStream(dml, testCharacterStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStream insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingInsertApproachZeroWithIntLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithIntLength(dml, testCharacterStreamArray, false,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithIntLength insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingInsertApproachZeroWithLongLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithLongLength(dml, testCharacterStreamArray, false,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithLongLength insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingInsert() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStream(dml, testCharacterStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStream insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingInsertWithIntLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithIntLength(dml, testCharacterStreamArray, false,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithIntLength insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingInsertWithLongLength() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithLongLength(dml, testCharacterStreamArray, false,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithLongLength insert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// update
	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpdateApproachZero() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStream(updateDml, testCharacterStreamArray, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStream update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpdateApproachZeroWithIntLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithIntLength(updateDml, testCharacterStreamArray, true,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithIntLength update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpdateApproachZeroWithLongLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithLongLength(updateDml, testCharacterStreamArray, true,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithLongLength update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpdate() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStream(updateDml, testCharacterStreamArray, true);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStream udpate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpdateWithIntLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithIntLength(updateDml, testCharacterStreamArray, true,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithIntLength update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpdateWithLongLength() {
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_double=? where c_id=?";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithLongLength(updateDml, testCharacterStreamArray, true,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithLongLength update value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert insert
	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpsertInsertApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStream(upsertDml, testCharacterStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStream upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpsertInsertApproachZeroWithIntLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithIntLength(upsertDml, testCharacterStreamArray, false,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithIntLength upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpsertInsertApproachZeroWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithLongLength(upsertDml, testCharacterStreamArray, false,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithLongLength upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpsertInsert() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStream(upsertDml, testCharacterStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStream upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpsertInsertWithIntLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithIntLength(upsertDml, testCharacterStreamArray, false,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithIntLength upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpsertInsertWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();

			testCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithLongLength(upsertDml, testCharacterStreamArray, false,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithLongLength upsertinsert value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// upsert update
	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpsertUpdateApproachZero() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStream(upsertDml, testCharacterStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStream upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpsertUpdateApproachZeroWithIntLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithIntLength(upsertDml, testCharacterStreamArray, false,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithIntLength upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpsertUpdateApproachZeroWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testCharacterStreamStringArray = new String[] { approachZero, minusApproachZero };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithLongLength(upsertDml, testCharacterStreamArray, false,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithLongLength upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpsertUpdate() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStream(upsertDml, testCharacterStreamArray, false);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStream udpate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpsertUpdateWithIntLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithIntLength(upsertDml, testCharacterStreamArray, false,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithIntLength upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testDoublePrecisionSetCharacterStreamByUsingUpsertUpdateWithLongLength() {
		String upsertDml = "upsert using load into  " + schema + "." + testDoubleTable + " values(?,?)";

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0),(2,2.0)");

			testCharacterStreamStringArray = new String[] { DOUBLE_MAX_VALUE, MINUS_DOUBLE_MAX_VALUE };
			testCharacterStreamArray = new StringReader[] { new StringReader((testCharacterStreamStringArray[0])),
					new StringReader((testCharacterStreamStringArray[1])) };
			try {
				insertCharacterStreamWithLongLength(upsertDml, testCharacterStreamArray, false,
						testCharacterStreamStringArray);
			} catch (SQLException e) {
				if (e.getMessage().contains("invalid_datatype_for_column")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}

			fail("use setCharacterStreamWithLongLength upsertupdate value to DoublePrecision column successfully");
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	// get
	@Test
	public void testDoublePrecisionGetCharacterStream() {
		String dml = null;

		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("insert into " + schema + "." + testDoubleTable + " values(1,1.0)");

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			Reader reader = rs.getCharacterStream(2);
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