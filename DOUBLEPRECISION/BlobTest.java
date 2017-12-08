package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.io.IOUtils;
import org.jdbctest.common.BaseTest;
import org.junit.Test;

public class BlobTest extends BaseTest {
	private Blob testBlob = null;

	@Test
	public void testSetBlobByUsingInsert() throws SQLException {
		String tableName = "insertDOUBLEPRECISIONTable_Blob";
		String ddl = null;
		String dml = "insert into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);

			testBlob = conn.createBlob();
			testBlob.setBytes(1, "88.7".getBytes());
			pstmt = conn.prepareStatement(dml);
			pstmt.setInt(1, 1);
			pstmt.setBlob(2, testBlob);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(Double.parseDouble("88.7") == rs.getDouble(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetBlobByUsingUpdate() throws SQLException {
		String tableName = "upateDOUBLEPRECISIONTable_Blob";
		String ddl = null;
		String dml = null;
		String updateDml = "update " + schema + "." + tableName + " set c_DOUBLEPRECISION= ? where c_ID = " + 1;

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);
			dml = "insert into " + schema + "." + tableName + " values(1, 88.7)";
			stmt.executeUpdate(dml);

			testBlob = conn.createBlob();
			testBlob.setBytes(1, "88.7".getBytes());
			pstmt = conn.prepareStatement(updateDml);
			pstmt.setBlob(1, testBlob);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(Double.parseDouble("88.7") == rs.getDouble(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetBlobByUsingUpsertInsert() throws SQLException {
		String tableName = "upsertInsertDOUBLEPRECISIONTable_Blob";
		String ddl = null;
		String dml = null;
		String upsertDml = "upsert using load into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);

			testBlob = conn.createBlob();
			testBlob.setBytes(1, "88.7".getBytes());
			pstmt = conn.prepareStatement(upsertDml);
			pstmt.setInt(1, 1);
			pstmt.setBlob(2, testBlob);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(Double.parseDouble("88.7") == rs.getDouble(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetBlobByUsingUpsertUpdate() throws SQLException {
		String tableName = "upsertUpdateDOUBLEPRECISIONTable_Blob";
		String ddl = null;
		String dml = null;
		String updateDml = "upsert using load into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);
			dml = "insert into " + schema + "." + tableName + " values(1, 88.7)";
			stmt.executeUpdate(dml);

			testBlob = conn.createBlob();
			testBlob.setBytes(1, "88.7".getBytes());
			pstmt = conn.prepareStatement(updateDml);
			pstmt.setInt(1, 1);
			pstmt.setBlob(2, testBlob);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(Double.parseDouble("88.7") == rs.getDouble(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testGetBlob() throws SQLException, IOException {
		String tableName = "selectDOUBLEPRECISIONTable_Blob";
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
			assertEquals("88.7", IOUtils.readLines(rs.getBlob(2).getBinaryStream(), "UTF-8").get(0).trim());
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}
}