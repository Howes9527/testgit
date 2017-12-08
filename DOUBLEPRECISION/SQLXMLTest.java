package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.Test;

public class SQLXMLTest extends BaseTest {
	private SQLXML testSQLXML = null;

	@Test
	public void testSetSQLXMLByUsingInsert() throws SQLException, IOException {
		String tableName = "insertDOUBLEPRECISIONTable_SQLXML";
		String ddl = null;
		String dml = "insert into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);

			testSQLXML = conn.createSQLXML();
			pstmt = conn.prepareStatement(dml);
			pstmt.setInt(1, 1);
			pstmt.setSQLXML(2, testSQLXML);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(testSQLXML.equals(rs.getSQLXML(2)));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetSQLXMLByUsingUpdate() throws SQLException {
		String tableName = "upateDOUBLEPRECISIONTable_SQLXML";
		String ddl = null;
		String dml = null;
		String updateDml = "update " + schema + "." + tableName + " set c_DOUBLEPRECISION= ? where c_ID = " + 1;

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);
			dml = "insert into " + schema + "." + tableName + " values(1, 1234.12345)";
			stmt.executeUpdate(dml);

			testSQLXML = conn.createSQLXML();
			pstmt = conn.prepareStatement(updateDml);
			pstmt.setSQLXML(1, testSQLXML);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(testSQLXML.equals(rs.getSQLXML(2)));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetSQLXMLByUsingUpsertInsert() throws SQLException {
		String tableName = "upsertInsertDOUBLEPRECISIONTable_SQLXML";
		String ddl = null;
		String dml = null;
		String upsertDml = "upsert using load into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);

			testSQLXML = conn.createSQLXML();
			pstmt = conn.prepareStatement(upsertDml);
			pstmt.setInt(1, 1);
			pstmt.setSQLXML(2, testSQLXML);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(testSQLXML.equals(rs.getSQLXML(2)));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetSQLXMLByUsingUpsertUpdate() throws SQLException {
		String tableName = "upsertUpdateDOUBLEPRECISIONTable_SQLXML";
		String ddl = null;
		String dml = null;
		String updateDml = "upsert using load into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);
			dml = "insert into " + schema + "." + tableName + " values(1, 1234.12345)";
			stmt.executeUpdate(dml);

			testSQLXML = conn.createSQLXML();
			pstmt = conn.prepareStatement(updateDml);
			pstmt.setInt(1, 1);
			pstmt.setSQLXML(2, testSQLXML);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(testSQLXML.equals(rs.getSQLXML(2)));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testGetSQLXML() throws SQLException {
		String tableName = "selectDOUBLEPRECISIONTable_SQLXML";
		String ddl = null;
		String dml = null;

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);
			dml = "insert into " + schema + "." + tableName + " values(1, 1234.12345)";
			stmt.executeUpdate(dml);

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertNotNull(rs.getSQLXML(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}
}