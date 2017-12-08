package org.jdbctest.datatype.DOUBLEPRECISION;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.jdbctest.common.BaseTest;
import org.junit.Test;

public class RefTest extends BaseTest {
	private Ref testRef = null;

	@Test
	public void testSetRefByUsingInsert() throws SQLException, IOException {
		String tableName = "insertDOUBLEPRECISIONTable_Ref";
		String ddl = null;
		String dml = "insert into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);

			testRef = getREF(getRandomString_num(4, 9) + "." + getRandomString_num(5, 9));
			pstmt = conn.prepareStatement(dml);
			pstmt.setInt(1, 1);
			pstmt.setRef(2, testRef);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(testRef.equals(rs.getRef(2)));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetRefByUsingUpdate() throws SQLException {
		String tableName = "upateDOUBLEPRECISIONTable_Ref";
		String ddl = null;
		String dml = null;
		String updateDml = "update " + schema + "." + tableName + " set c_DOUBLEPRECISION = ? where c_ID = " + 1;

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);
			dml = "insert into " + schema + "." + tableName + " values(1, 1234.12345)";
			stmt.executeUpdate(dml);

			testRef = getREF(getRandomString_num(4, 9) + "." + getRandomString_num(5, 9));
			pstmt = conn.prepareStatement(updateDml);
			pstmt.setRef(1, testRef);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(testRef.equals(rs.getRef(2)));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetRefByUsingUpsertInsert() throws SQLException {
		String tableName = "upsertInsertDOUBLEPRECISIONTable_Ref";
		String ddl = null;
		String dml = null;
		String upsertDml = "upsert using load into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);

			testRef = getREF(getRandomString_num(4, 9) + "." + getRandomString_num(5, 9));
			pstmt = conn.prepareStatement(upsertDml);
			pstmt.setInt(1, 1);
			pstmt.setRef(2, testRef);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(testRef.equals(rs.getRef(2)));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testSetRefByUsingUpsertUpdate() throws SQLException {
		String tableName = "upsertUpdateDOUBLEPRECISIONTable_Ref";
		String ddl = null;
		String dml = null;
		String updateDml = "upsert using load into " + schema + "." + tableName + " values(?, ?)";

		try (Statement stmt = conn.createStatement()) {
			ddl = "create table " + schema + "." + tableName
					+ " (c_id int not null,c_DOUBLEPRECISION DOUBLE PRECISION, primary key(c_id) )";
			stmt.executeUpdate(ddl);
			dml = "insert into " + schema + "." + tableName + " values(1, 1234.12345)";
			stmt.executeUpdate(dml);

			testRef = getREF(getRandomString_num(4, 9) + "." + getRandomString_num(5, 9));
			pstmt = conn.prepareStatement(updateDml);
			pstmt.setInt(1, 1);
			pstmt.setRef(2, testRef);
			pstmt.executeUpdate();
			pstmt.close();
			pstmt = null;

			dml = "select * from " + schema + "." + tableName;
			rs = stmt.executeQuery(dml);
			assertTrue(rs.next());
			assertTrue(testRef.equals(rs.getRef(2)));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	@Test
	public void testGetRef() throws SQLException {
		String tableName = "selectDOUBLEPRECISIONTable_Ref";
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
			assertNotNull(rs.getRef(2));
			rs.close();
			rs = null;
		} finally {
			cleanTestEnvironment(conn, tableName);
		}
	}

	private Ref getREF(final String floatString) throws SQLException {
		Ref testRef = new Ref() {

			@Override
			public void setObject(Object testOBject) throws SQLException {
				testOBject = Float.parseFloat(floatString);

			}

			@Override
			public Object getObject(Map<String, Class<?>> arg0) throws SQLException {
				return null;
			}

			@Override
			public Object getObject() throws SQLException {
				return Float.parseFloat(getRandomString_num(4, 9) + "." + getRandomString_num(5, 9));
			}

			@Override
			public String getBaseTypeName() throws SQLException {
				return Float.TYPE.toString();
			}
		};

		return testRef;
	}
}