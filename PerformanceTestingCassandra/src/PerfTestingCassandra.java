import java.sql.*;
import java.io.*;
import java.util.*; 
import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;

public class PerfTestingCassandra {
	
	public int TestItr = 1;
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
		ResultSet rs = table_meta.getTables(null, null, "%", null);  
		String Query = "SELECT COUNT(*) as {2} FROM {1}.{2}";
		ArrayList<String> tables =  new ArrayList<String>();
		while(rs.next()){
			String TC = rs.getString("TABLE_CAT");
			String TS = rs.getString("TABLE_SCHEM");
			String TN = rs.getString("TABLE_NAME");
//			try {
//				if (TN == "NodeIdInfo") continue;
//				GenuineQuery(MessageFormat.format(Query, "Cdata", TS, TN));
//			}
//			catch (SQLException e) {
//				 System.out.println("Error: " + TC +"."+TS+"."+ TN);
// 				 e.printStackTrace();
// 				 break;
//			}	  
		System.out.println("Table Name: " + TC +"."+TS+"."+ TN);
		}
		
		this.GenuineQuery("SELECT COUNT(*) as nyc_payroll_progress_testing FROM test.nyc_payroll_progress_testing");
		
		ResultSet rs_new = table_meta.getColumns(null, "test", "nyc_payroll_progress_testing", null);
	
	    while (rs_new.next()) {
	          System.out.println(rs_new.getString("COLUMN_NAME") + " "
	                    + rs_new.getString("TYPE_NAME") + " "
	                    + rs_new.getString("COLUMN_SIZE"));
	    }
	    System.out.println("\n");
	}
	
	public void BatchInsert(int numRecords) throws SQLException {
		
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
        	
//        	 To Not to Record outlier
//        	this.trailRun("SELECT COUNT(*) as NumRowsFound FROM cdata.test.sampletable");
    
     	
        	// --------------------- Cdata.test.utmfileanalyze_details --------------------
        	
        	TestItr = 3;
        	
//        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
//					+ " FROM test.nyc_payroll LIMIT 10000", driverType, debug);
//        	
//        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
//					+ " FROM test.nyc_payroll LIMIT 50000", driverType, debug);
//        	
//        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
//					+ " FROM test.nyc_payroll LIMIT 150000", driverType, debug);
//        	
//        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
//					+ " FROM test.nyc_payroll", driverType, debug);
        	
        	
        	// CDATA Spec
        	
        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
					+ " FROM Cdata.test.nyc_payroll LIMIT 10000", driverType, debug);
        	
        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
					+ " FROM Cdata.test.nyc_payroll LIMIT 50000", driverType, debug);
        	
        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
					+ " FROM Cdata.test.nyc_payroll LIMIT 150000", driverType, debug);
        	
        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, work_location_borough"
					+ " FROM Cdata.test.nyc_payroll", driverType, debug);
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
//		SQLObj.execCommand(Query);
//		obj.PerformExp(driverType, debug);
//		obj.GenuineQuery("SELECT * FROM test.keyspaces");
		obj.GenuineQuery("SELECT * FROM CData.system_schema.keyspaces");
//		SQLObj.execCommand("DELETE FROM Cdata.test.nyc_payroll WHERE mid_init = 'L' ");
//		obj.GenuineQuery("SELECT id, first_name, last_name, mid_init FROM Cdata.test.nyc_payroll");
//		obj.DBStats();
		SQLObj.terminate();	

//		TimeUnit.SECONDS.sleep(20);
		
//		driverType = "Progress Cassandra";
//		SQLObj = new ProgressAstraDriver("/home/vivekreddy/Desktop/Intern/secure-connect-testservers/");
//		obj = new PerfTestingCassandra(driverType); 
//		SQLObj.terminate();	
		
    }
}

class CDATAAstraDriver extends ConnectDB{
	
	String rootpath;
	
	CDATAAstraDriver(String path){
		rootpath = path;
	}
	
	public void connect() {
		
		Properties connProps = new Properties();
		
		connProps.setProperty("Server", "54a50177-db08-41a7-8436-75e761079623-us-east1.db.astra.datastax.com");
		connProps.setProperty("Port", "31364");
		connProps.setProperty("Database", "cdata");
		connProps.setProperty("User", "vivekk");
		connProps.setProperty("Password", "!rssbus2020");
		
		connProps.setProperty("Use SSL", "true");
		connProps.setProperty("SSL Server Cert", rootpath+"ca.crt");
		connProps.setProperty("SSL Client Cert Type", "PEMKEY_FILE");
		connProps.setProperty("SSL Client Cert", rootpath+"cert");
		connProps.setProperty("SSL Client Cert Password", "1WMDb49EicZdJo82w");
		
		
		try {
	        Class.forName("cdata.jdbc.cassandra.CassandraDriver");
	    }
	    catch (Exception exp) {
	        System.out.println("Sorry!!! Unable to load Cassandra Driver");
	        exp.printStackTrace();
	    }
		
		try {
	        conn = DriverManager.getConnection("jdbc:cassandra:", connProps);
	        
	        System.out.println("Connection Successful to Cassandra DB !!!!!!!!!!!!!!!!!!");
	    }
	    catch (Exception e) {
	        e.printStackTrace();
	    }
	}
}


class ProgressAstraDriver extends ConnectDB{
	
	String rootpath;
	
	ProgressAstraDriver(String path){
		rootpath = path;
	}
	
	public void connect() {
		
		Properties connProps = new Properties();
		
		connProps.setProperty("ValidateServerCertificate", "true");
		connProps.setProperty("Encryption", "SSL");
		connProps.setProperty("HostNameInCertificate", "54a50177-db08-41a7-8436-75e761079623-us-east1.db.astra.datastax.com");
//		connProps.setProperty("PortNumber", "");
		connProps.setProperty("KeyspaceName", "cdata");
		connProps.setProperty("User", "vivekk");
		connProps.setProperty("Password", "!rssbus2020");
		connProps.setProperty("LoginTimeout", "50");
		
		connProps.setProperty("TrustStore", rootpath+"trustStore.jks");
		connProps.setProperty("TrustStorePassword", "aInC913jf7yP0UBMA");
		connProps.setProperty("KeyStore", rootpath+"identity.jks");
		connProps.setProperty("KeyStorePassword", "WUVKy4jP8peM5S03i");
		connProps.setProperty("KeyPassword", "1WMDb49EicZdJo82w");
		
		
		try {
	        Class.forName("cdata.jdbc.cassandra.CassandraDriver");
	    }
	    catch (Exception exp) {
	        System.out.println("Sorry!!! Unable to load Cassandra Driver");
	        exp.printStackTrace();
	    }
		
		try {
	        conn = DriverManager.getConnection("jdbc:datadirect:cassandra://54a50177-db08-41a7-8436-75e761079623-us-east1.db.astra.datastax.com:31364;", connProps);
	        
	        System.out.println("Connection Successful to Cassandra DB !!!!!!!!!!!!!!!!!!");
	    }
	    catch (Exception e) {
	        e.printStackTrace();
	    }
	}
}


