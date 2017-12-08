package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.Test;

public class ArrayTest extends BaseTest {
	private Array testArray = null;

	@Test
	public void testSetArrayByUsingInsert() throws SQLException, IOException {
		String tableName = "insertDOUBLEPRECISIONTable_Array";
		String ddl = null;
		String dml = "insert into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);

			Object[] elements = { Float.parseFloat(getRandomString_num(9, 9) + "." + getRandomString_num(5, 9)) };
			testArray = conn.createArrayOf("java.lang.Float", elements);
			pstmt = conn.prepareStatement(dml);
			pstmt.setInt(1, 1);
			pstmt.setArray(2, testArray);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertNotNull(rs.getDouble(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetArrayByUsingUpdate() throws SQLException {
		String tableName = "upateDOUBLEPRECISIONTable_Array";
		String ddl = null;
		String dml = null;
		String updateDml = "update " + schema + "." + tableName + " set c_DOUBLEPRECISION= ? where c_ID = " + 1;

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);
			dml = "insert into " + schema + "." + tableName + " values(1, 123456.12345)";
			stmt.executeUpdate(dml);

			Object[] elements = { Float.parseFloat(getRandomString_num(9, 9) + "." + getRandomString_num(5, 9)) };
			testArray = conn.createArrayOf("java.lang.Float", elements);
			pstmt = conn.prepareStatement(updateDml);
			pstmt.setArray(1, testArray);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertNotNull(rs.getDouble(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetArrayByUsingUpsertInsert() throws SQLException {
		String tableName = "upsertInsertDOUBLEPRECISIONTable_Array";
		String ddl = null;
		String dml = null;
		String upsertDml = "upsert using load into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);

			Object[] elements = { Float.parseFloat(getRandomString_num(9, 9) + "." + getRandomString_num(5, 9)) };
			testArray = conn.createArrayOf("java.lang.Float", elements);
			pstmt = conn.prepareStatement(upsertDml);
			pstmt.setInt(1, 1);
			pstmt.setArray(2, testArray);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertNotNull(rs.getDouble(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetArrayByUsingUpsertUpdate() throws SQLException {
		String tableName = "upsertUpdateDOUBLEPRECISIONTable_Array";
		String ddl = null;
		String dml = null;
		String updateDml = "upsert using load into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);
			dml = "insert into " + schema + "." + tableName + " values(1, 123456.12345)";
			stmt.executeUpdate(dml);

			Object[] elements = { Float.parseFloat(getRandomString_num(9, 9) + "." + getRandomString_num(5, 9)) };
			testArray = conn.createArrayOf("java.lang.Float", elements);
			pstmt = conn.prepareStatement(updateDml);
			pstmt.setInt(1, 1);
			pstmt.setArray(2, testArray);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertNotNull(rs.getDouble(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testGetArray() throws SQLException {
		String tableName = "selectDOUBLEPRECISIONTable_Array";
		String ddl = null;
		String dml = null;

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);
			dml = "insert into " + schema + "." + tableName + " values(1, 123456.12345)";
			stmt.executeUpdate(dml);

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertNotNull(rs.getArray(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}
}