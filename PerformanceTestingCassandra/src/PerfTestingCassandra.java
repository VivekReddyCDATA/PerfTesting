
import java.sql.*;
import java.io.*;
import java.util.*; 
import java.text.MessageFormat;
import java.util.Random; 
import java.util.concurrent.TimeUnit;


public class PerfTestingCassandra {
	
	public int TestItr = 5;
	public static ConnectDB SQLObj;
	
	public PerfTestingCassandra(String driverType)
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
		String Query = "SELECT COUNT(*) FROM {0}";
		ArrayList<String> tables =  new ArrayList<String>();
		while(rs.next()){
			tables.add(rs.getString("TABLE_NAME"));  
		}
		
		for (String t_name : tables) {
//			  if (!t_name.matches(".*[| __Tag | __History | __Share | __x]")) {
//				  try {
//					  GenuineQuery(MessageFormat.format(Query, t_name));  
//				  }
//				  catch (SQLException e) {
//					  e.printStackTrace();
//				  }	  
//			  }	
			System.out.println(t_name);
		}
		
//		rs = table_meta.getColumns(null, null, "MyCustomObject__c", null);
//      System.out.println("MyCustomObject__c");
//      while (rs.next()) {
//            System.out.println(rs.getString("COLUMN_NAME") + " "
//                    + rs.getString("TYPE_NAME") + " "
//                    + rs.getString("COLUMN_SIZE"));
//      }
//      System.out.println("\n");
	}
	
	public void BatchInsert(int numRecords) throws SQLException{
		
		Random rand = new Random();
		
		String Query =  "INSERT INTO MyCustomObject__c (Name, CustomString__c, "
						+ "CustomString2__c, CustomString3__c, CustomDateTime1__c, " 
						+ "CustomDateTime2__c, CustomDouble1__c)"
						+ " VALUES (?, ?, ?, ?, ?, ?, ?) ";
		PreparedStatement pstmt = SQLObj.getPreparedStatement(Query);
		
		for (int k = 0; k < numRecords; k++) {
			String i = String.valueOf(k);
			pstmt.setString(1, "Test Object 1 : " + i );
			pstmt.setString(2, "Test String 'Just Some Random Stuffffff' 1 : " + i );
			pstmt.setString(3, "Test String 'Just Similar Ramdom Stufffffffff' 2 : " + i );
			pstmt.setString(4, "Test String 'HIiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii" 
								+ "iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii' 3 : " + i );
			
			pstmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
			pstmt.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
			pstmt.setDouble(7, rand.nextDouble());
			pstmt.addBatch();
		}
		
		
		int[] r = pstmt.executeBatch();
		for(int j: r)
		  System.out.println(j);
			
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
			java.util.Date dEnd = new java.util.Date();  //get end time
			ldiff.add(dEnd.getTime()-dStart.getTime());
		}
		Long avg = Long.valueOf(0);
	    for (int i = 0; i<TestItr; i++) { avg += ldiff.get(i);}
	    avg = avg / (TestItr);
	    System.out.print(MessageFormat.format("Query Time (ms): {0}, Driver Type: {1} \n .......Query \"{2}\"\n", avg, driverType, cmd)); 	
	}
	
	
	public void PerformExp(String driverType, boolean debug) {
		
        try {
        	
        	// obj.DBStats();
    
        	// --------------------- Lead Table -------------------------
        	
        	// To Not to Record Outlier
        	this.trailRun("SELECT COUNT(*) as NumRowsFound FROM Lead");
     
        	this.statistics("SELECT LastViewedDate FROM Lead", driverType, debug);
        	
        	this.statistics("SELECT Website FROM Lead", driverType, debug);
        	
        	this.statistics("SELECT Street, City, State, Latitude , Longitude FROM Lead", driverType, debug);
        	
        	this.statistics("SELECT Website, MobilePhone, Description, LastViewedDate, " 
        					+ "Street, City, State, Latitude, Longitude, LastReferencedDate " 
        					+ "FROM Lead", driverType, debug);
        	
        	// --------------------  Custom Table ------------------------------------
//        	this.BatchInsert(2500);
        	
//        	// To Not to Record Outlier
//        	this.trailRun("SELECT COUNT(*) FROM MyCustomObject__c");
//        	
//        	// Tests
//        	this.statistics("SELECT Name, CustomString__c, " 
//        			+ "CustomString2__c, CustomString3__c, CustomDateTime1__c, " 
//        			+ "CustomDateTime2__c, CustomDouble1__c FROM MyCustomObject__c", driverType, debug);  	
        }
        
        catch (Exception e) {
            System.out.println("DB: Unable to close the statement \n" + e);
        }   
	}
	
	public static void main(String[] args) throws Exception
    {	
		String driverType;
		PerfTestingCassandra obj;
		
		boolean debug = false;
		
		System.out.println("\n ------------------ CDATA Section -----------------");
		driverType = "Cassandra";
        SQLObj = new CDATACassandraDriver();
		obj = new PerfTestingCassandra(driverType); 
		obj.DBStats();
//		obj.PerformExp(driverType, debug);
		SQLObj.terminate();	
//		
//		TimeUnit.SECONDS.sleep(20);
//		
		System.out.println("\n ------------------ Progress Section -----------------");
//        
//      driverType = "Progress";
//      SQLObj = new ProgressDriver();
//		obj = new perfTest(driverType); 
//		obj.PerformExp(driverType, debug);
//		SQLObj.terminate();		
    }
}

class CDATACassandraDriver extends ConnectDB{
	
	public void connect() {
		try {
	        Class.forName("cdata.jdbc.cassandra.CassandraDriver");
	    }
	    catch (Exception exp) {
	        System.out.println("Sorry!!! Unable to load Cassandra Driver");
	        exp.printStackTrace();
	    }
	    
	    try {
	        conn = DriverManager.getConnection("jdbc:cassandra:server=localhost;port=9042;");
	        
	        System.out.println("Connection Successful to SalesForce API !!!!!!!!!!!!!!!!!!");
	    }
	    catch (Exception e) {
	        e.printStackTrace();
	    }
	}
	
}
