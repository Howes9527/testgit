package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.Test;

public class RowIdTest extends BaseTest {
	private RowId testRowId = null;

	@Test
	public void testSetRowIdByUsingInsert() throws SQLException {
		String tableName = "insertDOUBLEPRECISIONTable_RowId";
		String ddl = null;
		String dml = "insert into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);

			pstmt = conn.prepareStatement(dml);
			testRowId = new RowId() {
				@Override
				public byte[] getBytes() {
					return getRandomString_num(5, 9).getBytes();
				}
			};
			pstmt.setInt(1, 1);
			pstmt.setRowId(2, testRowId);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertEquals(testRowId, rs.getRowId(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetRowIdByUsingUpdate() throws SQLException {
		String tableName = "upateDOUBLEPRECISIONTable_RowId";
		String ddl = null;
		String dml = null;
		String updateDml = "update " + schema + "." + tableName + " set c_DOUBLEPRECISION= ? where c_ID = " + 1;

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);
			dml = "insert into " + schema + "." + tableName + " values(1, 88.7)";
			stmt.executeUpdate(dml);

			pstmt = conn.prepareStatement(updateDml);
			testRowId = new RowId() {
				@Override
				public byte[] getBytes() {
					return getRandomString_num(5, 9).getBytes();
				}
			};
			pstmt.setRowId(1, testRowId);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertEquals(testRowId, rs.getRowId(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetRowIdByUsingUpsertInsert() throws SQLException {
		String tableName = "upsertInsertDOUBLEPRECISIONTable_RowId";
		String ddl = null;
		String dml = null;
		String upsertDml = "upsert using load into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);

			pstmt = conn.prepareStatement(upsertDml);
			testRowId = new RowId() {
				@Override
				public byte[] getBytes() {
					return getRandomString_num(5, 9).getBytes();
				}
			};
			pstmt.setInt(1, 1);
			pstmt.setRowId(2, testRowId);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertEquals(testRowId, rs.getRowId(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetRowIdByUsingUpsertUpdate() throws SQLException {
		String tableName = "upsertUpdateDOUBLEPRECISIONTable_RowId";
		String ddl = null;
		String dml = null;
		String updateDml = "upsert using load into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);
			dml = "insert into " + schema + "." + tableName + " values(1, 88.7)";
			stmt.executeUpdate(dml);

			pstmt = conn.prepareStatement(updateDml);
			testRowId = new RowId() {
				@Override
				public byte[] getBytes() {
					return getRandomString_num(5, 9).getBytes();
				}
			};
			pstmt.setInt(1, 1);
			pstmt.setRowId(2, testRowId);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertEquals(testRowId, rs.getRowId(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testGetRowId() throws SQLException {
		String tableName = "selectDOUBLEPRECISIONTable_RowId";
		String ddl = null;
		String dml = null;

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);
			dml = "insert into " + schema + "." + tableName + " values(1, 88.7)";
			stmt.executeUpdate(dml);

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertNotNull(rs.getRowId(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}
}