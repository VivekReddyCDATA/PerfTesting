import java.sql.*;
import java.io.*;
import java.util.*; 
import java.text.MessageFormat;

public class perfTest {
	
	public int TestItr = 5;
	public static ConnectDB SQLObj;
	
	public perfTest(String driverType)
	{		
		long ldiff; 
		java.util.Date dStart = new java.util.Date();  //get start time
		SQLObj.connect();
		java.util.Date dEnd = new java.util.Date();  //get end time
	    ldiff = dEnd.getTime()-dStart.getTime();
	    System.out.print(MessageFormat.format("Database connection time (ms): {0}, Driver Type: {1}\n", ldiff, driverType));
	   	
	}
	
	
	public void DBStats() throws SQLException{
		DatabaseMetaData table_meta = SQLObj.getMetaData();
		String table[]={"TABLE"};  
		ResultSet rs = table_meta.getTables(null, null, "%", table);  
		String Query = "SELECT COUNT(*) as {0} FROM {0}";
		ArrayList<String> tables =  new ArrayList<String>();
		while(rs.next()){
			tables.add(rs.getString("TABLE_NAME"));  
		}
		for (String t_name : tables) {
			  if (!t_name.matches(".*[| __Tag | __History | __Share | __x]")) {
				  try {
					  GenuineQuery(MessageFormat.format(Query, t_name));  
				  }
				  catch (SQLException e) {
					  System.out.println("Error: " + t_name);
				  }	  
			  }	
		}
	}
	
	public void GenuineQuery(String cmd) throws SQLException{
		ResultSet rs = SQLObj.execQuery(cmd);
		while(rs.next()) {
		    for(int i=1;i<=rs.getMetaData().getColumnCount();i++) {
		    	  System.out.println(rs.getMetaData().getColumnName(i) +" = "+rs.getString(i)); 
		    }
		}
	}
	
	public void trailRun(String cmd) throws SQLException
	{
		java.util.Date dStart = new java.util.Date();   //get start time
		this.GenuineQuery(cmd);
		java.util.Date dEnd = new java.util.Date();  //get end time
		System.out.print(MessageFormat.format("Trail Run: Query Time (ms): {0}, .Query \"{1}\"\n", dEnd.getTime()-dStart.getTime(), cmd)); 
	}
	
	public void statistics(String cmd, String driverType, boolean debug) throws SQLException
	{	
		int numRows = 0;
		ArrayList<Long> ldiff = new ArrayList<Long>();
		for (int iter = 0; iter < TestItr; iter++) {
	        java.util.Date dStart = new java.util.Date();   //get start time
			ResultSet rs = SQLObj.execQuery(cmd);
			while(rs.next()) {
			    for(int i=1;i<=rs.getMetaData().getColumnCount();i++) {
			      if (iter == 0 && debug) {
			    	  System.out.println(rs.getMetaData().getColumnName(i) +"="+rs.getString(i)); 
			      }
			      else {
			    	  Object obj = rs.getObject(i);
			      }
			    }
			}
			numRows = rs.getRow();
			java.util.Date dEnd = new java.util.Date();  //get end time
			ldiff.add(dEnd.getTime()-dStart.getTime());
		}
		Long avg = Long.valueOf(0);
	    for (int i = 0; i<TestItr; i++) { avg += ldiff.get(i);}
	    avg = avg / (TestItr);
	    System.out.print(MessageFormat.format("Query Time (ms): {0}, Driver Type: {1} Num Rows {2} \n .......Query \"{3}\"\n", avg, driverType, numRows, cmd)); 	
	}

	public static void main(String[] args) throws Exception
    {	
		boolean debug = false;
		
		// --------------------------------------  CDATA Section -----------------------------------------
		String driverType = "CData";
		SQLObj = new CDATADriver();
		
        perfTest obj = new perfTest(driverType);   
        try {
        	
        	obj.DBStats();
//        	// To Not to Record Outlier
//        	obj.trailRun("SELECT COUNT(*) as LeadFeed FROM LeadFeed");
//        	
//        	// Tests
//        	obj.statistics("SELECT LastViewedDate FROM Lead", driverType, debug);
//        	obj.statistics("SELECT Website FROM Lead", driverType, debug);
//        	obj.statistics("SELECT Street, City, State, Latitude , Longitude FROM Lead", driverType, debug);
        }
        catch (Exception e) {
            System.out.println("DB: Unable to close the statement" + e);
        }
        SQLObj.terminate();
        
     // --------------------------------------  Competitor 1 -----------------------------------------
        
        
        
        
     // --------------------------------------  Competitor 2 -----------------------------------------
    }
}

class CDATADriver extends ConnectDB {
	
	private String user = "support@nsoftware.com";
	private String pwd = "!rssbus";
	private String Token = "zITU0ThaYMAFudbDvl4Vj1gH";
	private String URL = "jdbc:cdata:salesforce:";
	
	public void connect() {
		
		DataSourceURL = URL;
		
		Properties prop = new Properties();
		
	    try {
	    	prop.setProperty("User" , user);
	        prop.setProperty("Password", pwd);
	        prop.setProperty("Security Token", Token);
	        Class.forName("cdata.jdbc.salesforce.SalesforceDriver");
	    }
	    catch (Exception exp) {
	        System.out.println("Sorry!!! Unable to load SalesForce Driver");
	        exp.printStackTrace();
	    }
	    
	    try {
	        conn = DriverManager.getConnection(DataSourceURL, prop);
	        System.out.println("Connection Successful to SalesForce API !!!!!!!!!!!!!!!!!!");
	    }
	    catch (Exception e) {
	        System.out.println(e);
	    }
	}
}
