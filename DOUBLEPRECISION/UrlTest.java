package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.sql.Statement;

import org.jdbctest.common.BaseTest;
import org.junit.Test;

public class UrlTest extends BaseTest {
	private URL testUrl = null;
	@Test
	public void testSetURLByUsingInsert() throws SQLException, IOException {
		String tableName = "insertDOUBLEPRECISIONTable_URL";
		String ddl = null;
		String dml = "insert into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);

			testUrl = new URL("http://www.baidu.com/");
			pstmt = conn.prepareStatement(dml);
			pstmt.setInt(1, 1);
			pstmt.setURL(2, testUrl);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(testUrl.equals(rs.getURL(2)));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetURLByUsingUpdate() throws SQLException, MalformedURLException {
		String tableName = "upateDOUBLEPRECISIONTable_URL";
		String ddl = null;
		String dml = null;
		String updateDml = "update " + schema + "." + tableName + " set c_DOUBLEPRECISION= ? where c_ID = " + 1;

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);
			dml = "insert into " + schema + "." + tableName + " values(1, 1234.12345)";
			stmt.executeUpdate(dml);

			testUrl = new URL("http://www.baidu.com/");
			pstmt = conn.prepareStatement(updateDml);
			pstmt.setURL(1, testUrl);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(testUrl.equals(rs.getURL(2)));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetURLByUsingUpsertInsert() throws SQLException, MalformedURLException {
		String tableName = "upsertInsertDOUBLEPRECISIONTable_URL";
		String ddl = null;
		String dml = null;
		String upsertDml = "upsert using load into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);

			testUrl = new URL("http://www.baidu.com/");
			pstmt = conn.prepareStatement(upsertDml);
			pstmt.setInt(1, 1);
			pstmt.setURL(2, testUrl);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(testUrl.equals(rs.getURL(2)));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetURLByUsingUpsertUpdate() throws SQLException, MalformedURLException {
		String tableName = "upsertUpdateDOUBLEPRECISIONTable_URL";
		String ddl = null;
		String dml = null;
		String updateDml = "upsert using load into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);
			dml = "insert into " + schema + "." + tableName + " values(1, 1234.12345)";
			stmt.executeUpdate(dml);

			testUrl = new URL("http://www.baidu.com/");
			pstmt = conn.prepareStatement(updateDml);
			pstmt.setInt(1, 1);
			pstmt.setURL(2, testUrl);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(testUrl.equals(rs.getURL(2)));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testGetURL() throws SQLException {
		String tableName = "selectDOUBLEPRECISIONTable_URL";
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
			assertNotNull(rs.getURL(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}
}