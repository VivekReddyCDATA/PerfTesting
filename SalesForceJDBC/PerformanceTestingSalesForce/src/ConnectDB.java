import java.sql.*;
//import org.apache.ibatis.jdbc.ScriptRunner;
// https://www.geeksforgeeks.org/establishing-jdbc-connection-in-java/ (Resources to understand JDBC Connectivity)
import java.util.Properties;
public class ConnectDB {
 
	protected static final String SalesForceURL = "jdbc:salesforce:";

	protected static Connection conn = null;
    protected static Statement stmt = null;
    
    protected Properties prop = new Properties();
    
    public ConnectDB()
    {
    try {
    	prop.setProperty("User" , "support@nsoftware.com");
        prop.setProperty("Password","!rssbus");
        prop.setProperty("Security Token","zITU0ThaYMAFudbDvl4Vj1gH");
        Class.forName("cdata.jdbc.salesforce.SalesforceDriver");
        }
    catch (Exception exp) {
        System.out.println("Sorry!!! Unable to load SalesForce Driver");
        exp.printStackTrace();
    }
    }

   public void connect(){
	
	
    try {
        conn = DriverManager.getConnection(SalesForceURL, prop);
        System.out.println("Connection Successful to SalesForce API !!!!!!!!!!!!!!!!!!");
    }
    catch (Exception e) {
        System.out.println(e);
    }
    
    }

    // Ask any OODD Student for the difference between a Query and a Command.
    // ResultSet in the Java abstraction of records received from the Database.
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

