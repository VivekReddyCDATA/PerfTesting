import java.sql.*;

import java.util.Properties;

abstract class ConnectDB {
 
	protected static String DataSourceURL;  //= "";

	protected static Connection conn = null;
    protected static Statement stmt = null;
     
    abstract public void connect(); 
    
    protected String user = "vivekk@cdata101.dev";
    protected String pwd = "!rssbus2020";
    protected String Token = "TTcwRPyZ2piuMnI8wYQXzn5lJ";
    
    public DatabaseMetaData getMetaData() throws SQLException{
    	if (conn == null) {
    		throw new SQLException("Please open a connection to extract MetaData");
    	}
    	DatabaseMetaData table_meta = conn.getMetaData();
    	return table_meta;
    }
    
    public PreparedStatement getPreparedStatement(String Query) throws SQLException {
    	PreparedStatement pstmt = conn.prepareStatement(Query,Statement.RETURN_GENERATED_KEYS);
    	return pstmt;
    }
    
    public ResultSet execQuery(String cmd) throws SQLException {
	     if (conn == null)
	     {
	    	 throw new SQLException("Please open a connection to execute a Query");
	     }
	     stmt = conn.createStatement();
	     boolean ret = stmt.execute(cmd);
	     
	     ResultSet rs = null;
	     if (ret) {
	    	 rs=stmt.getResultSet();
	     	}
	     else {
	    	 System.out.println("Nothing has been Returned by Connection Object, NUll Object is returned");
	     	}
	     return rs;
	     }

    public void execCommand(String cmd) throws SQLException {
     if (conn == null)
     {
         throw new SQLException("Please open a connection to execute a Query");
     }
     stmt = conn.createStatement();
     stmt.executeUpdate(cmd);
     stmt.close();
    }

    public void terminate() {

        if (stmt != null)
        {
             try {
                stmt.close();
            }
            catch (Exception e) {
                System.out.println("DB: Unable to close the statement");
            }
        }
        if (conn != null) {
            try {
                conn.close();
                System.out.println("Connection to the DataSource is closed");
            }
            catch (Exception e) {
                System.out.println("DB: Unable to close the connection");
            }
        }
    }
}

