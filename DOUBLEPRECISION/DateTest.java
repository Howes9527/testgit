package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class DateTest extends BaseTest {
	private Date testDate = null;
	private long testLong;
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
	public void testSetDateByUsingInsert() {
		String dml = "insert into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testLong = new java.util.Date().getTime();
			testDate = new Date(testLong);
			pstmt = conn.prepareStatement(dml);
			pstmt.setInt(1, 1);
			try {
				pstmt.setDate(2, testDate);
			} catch (SQLException e) {
				if (e.getMessage().contains("Restricted data type attribute violation")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertNotNull(rs.getDouble(2));
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testSetDateByUsingUpdate() {
		String dml = null;
		String updateDml = "update " + schema + "." + testDoubleTable + " set c_DOUBLE= ? where c_ID = " + 1;

		try {
			stmt = conn.createStatement();
			dml = "insert into " + schema + "." + testDoubleTable + " values(1, 1234.1234)";
			stmt.executeUpdate(dml);

			testLong = new java.util.Date().getTime();
			testDate = new Date(testLong);
			pstmt = conn.prepareStatement(updateDml);
			try {
				pstmt.setDate(1, testDate);
			} catch (SQLException e) {
				if (e.getMessage().contains("Restricted data type attribute violation")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertNotNull(rs.getDouble(2));
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testSetDateByUsingUpsertInsert() {
		String dml = null;
		String upsertDml = "upsert using load into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();

			testLong = new java.util.Date().getTime();
			testDate = new Date(testLong);
			pstmt = conn.prepareStatement(upsertDml);
			pstmt.setInt(1, 1);
			try {
				pstmt.setDate(2, testDate);
			} catch (SQLException e) {
				if (e.getMessage().contains("Restricted data type attribute violation")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertNotNull(rs.getDouble(2));
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testSetDateByUsingUpsertUpdate() {
		String dml = null;
		String updateDml = "upsert using load into " + schema + "." + testDoubleTable + " values(?, ?)";

		try {
			stmt = conn.createStatement();
			dml = "insert into " + schema + "." + testDoubleTable + " values(1, 1234.1234)";
			stmt.executeUpdate(dml);

			testLong = new java.util.Date().getTime();
			testDate = new Date(testLong);
			pstmt = conn.prepareStatement(updateDml);
			pstmt.setInt(1, 1);
			try {
				pstmt.setDate(2, testDate);
			} catch (SQLException e) {
				if (e.getMessage().contains("Restricted data type attribute violation")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertNotNull(rs.getDouble(2));
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}

	@Test
	public void testGetDate() {
		String dml = null;

		try {
			stmt = conn.createStatement();
			dml = "insert into " + schema + "." + testDoubleTable + " values(1, 1234.1234)";
			stmt.executeUpdate(dml);

			dml = "select * from " + schema + "." + testDoubleTable;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			try {
				assertNotNull(rs.getDate(2));
			} catch (SQLException e) {
				if (e.getMessage().contains("Restricted data type attribute violation")) {
					return;
				} else {
					e.printStackTrace();
					fail(e.getMessage());
				}
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
			fail(e1.getMessage());
		} finally {
			cleanTestData(conn, testDoubleTable);
		}
	}
}
