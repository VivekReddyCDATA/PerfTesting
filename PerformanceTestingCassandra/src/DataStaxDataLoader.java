import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.internal.core.os.Libc;
import com.fasterxml.jackson.databind.ObjectMapper;


public class DataStaxDataLoader {
	
	public int TestItr = 3;
	
	public CqlSession session;
	
	public String TABLE_NAME = "nyc_payroll";
	
	public String keyspace;
	
	public DataStaxDataLoader(CqlSession sess, String key) {
		session = sess;
		keyspace = key;
	}
	

	public void createTable() {
        CreateTable createTable = SchemaBuilder.createTable(TABLE_NAME)
          .withPartitionKey("id", DataTypes.UUID)
          .withColumn("title_description", DataTypes.TEXT)
          .withColumn("agency_start_date", DataTypes.TIMESTAMP)
          .withColumn("last_name", DataTypes.TEXT)
          .withColumn("regular_gross_paid", DataTypes.DOUBLE)
          .withColumn("total_other_pay", DataTypes.DOUBLE);
        
        this.executeStatement(createTable.build());
    }
 
    public ResultSet executeStatement(SimpleStatement statement) {
        return session.execute(statement);
    }
    
    public void statistics(String cmd, String driverType, boolean debug) throws SQLException
	{	
    	List<Row> obj = null;
    	int numRows = 0; 
		ArrayList<Long> ldiff = new ArrayList<Long>();
		for (int iter = 0; iter < TestItr; iter++) {
	        java.util.Date dStart = new java.util.Date();   //get start time
			ResultSet rs = session.execute(cmd);
			while(rs.isFullyFetched()) {
//			    for(int i=1;i<=rs.forEach(action); i++) {
//			      if (iter == 0 && debug) {
//			    	  System.out.println(rs.getMetaData().getColumnName(i) +"="+rs.getString(i)); 
//			      }
//			      else {
			    	 
//			      }
//			    }
			}
			
			Iterator<Row> itr = rs.iterator();
			while(itr.hasNext()) {
				itr.next();
				numRows++;
			}
			
			java.util.Date dEnd = new java.util.Date();  //get end time
			ldiff.add(dEnd.getTime()-dStart.getTime());
		}
		Long avg = Long.valueOf(0);
	    for (int i = 0; i<TestItr; i++) { avg += ldiff.get(i);}
	    avg = avg / (TestItr);
	    System.out.print(MessageFormat.format("Query Time (ms): {0}, NumRows: {3}, Driver Type: {1} \n .......Query \"{2}\"\n", avg, driverType, cmd, numRows)); 	
	}
    
    public void PerformExp(String driverType, boolean debug) {
		
        try {
        	
//        	 To Not to Record outlier
//        	this.trailRun("SELECT COUNT(*) as NumRowsFound FROM cdata.test.sampletable");
    
     	
        	// --------------------- Cdata.test.utmfileanalyze_details --------------------
        	
        	
        	
//        	
//        	

//        
        	
        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, agency_start_date"
					+ " FROM cdata.nyc_payroll", driverType, debug);
        	
        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, agency_start_date"
					+ " FROM cdata.nyc_payroll LIMIT 150000", driverType, debug);
        	
        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, agency_start_date"
					+ " FROM cdata.nyc_payroll LIMIT 50000", driverType, debug);
        	
        	this.statistics("SELECT id, last_name, title_description, regular_gross_paid, total_other_pay, agency_start_date"
					+ " FROM cdata.nyc_payroll LIMIT 10000", driverType, debug);
        }
        
        catch (Exception e) {
            System.out.println("DB: Unable to close the statement \n" + e);
        }   
	}
    
    public void batchInsertFromJson(String path) {
    	
    	ObjectMapper mapper = new ObjectMapper();
    	PayRoll[] payrollObjects = null;
    	
    	try {		
			payrollObjects = mapper.readValue(new File(path), PayRoll[].class);      
        } 
		catch (IOException e) {
            e.printStackTrace();
        }
    	
    	int numRecords = payrollObjects.length;
		
		int numRecordsPerUpdate = 1000;
		int UpdateCount = numRecords/numRecordsPerUpdate;
		
		assert UpdateCount*numRecordsPerUpdate == numRecords : " Non Divisible by " + 1000 + " Error";
    	
		
		java.util.Date dStart = new java.util.Date();
		
//		PreparedStatement pstmt =
//			    session.prepare(
//			        MessageFormat.format("INSERT INTO {0}.{1} (id, title_description, agency_start_date, total_other_pay, regular_gross_paid, last_name)"
//				            + "VALUES (:id, :title, :date, :other_pay, :paid, :name)", keyspace, TABLE_NAME));
		
		RegularInsert insertInto = QueryBuilder.insertInto(TABLE_NAME)
				 		  .value("id", QueryBuilder.bindMarker())
				 		  .value("title_description", QueryBuilder.bindMarker())
				 		  .value("agency_start_date", QueryBuilder.bindMarker())
				 		  .value("total_other_pay", QueryBuilder.bindMarker())
				 		  .value("regular_gross_paid", QueryBuilder.bindMarker())
				 		  .value("last_name", QueryBuilder.bindMarker());
		
		SimpleStatement insertStatement = insertInto.build();
		
		insertStatement = insertStatement.setKeyspace(keyspace);
		
		PreparedStatement pstmt = session.prepare(insertStatement);
		
		BatchStatement bstmt; // = BatchStatement.newInstance(DefaultBatchType.UNLOGGED);
		
		bstmt = BatchStatement.newInstance(DefaultBatchType.UNLOGGED);
		
		try {	
			for (int j = 0; j < numRecords; j++) {
				
				bstmt = BatchStatement.newInstance(DefaultBatchType.UNLOGGED);
				
				for (int k = 0; k < numRecordsPerUpdate; k++) {
					
					int i = j*numRecordsPerUpdate + k;
				
					PayRoll obj = payrollObjects[i];
					
//					ldt = LocalDateTime.parse(obj.agency_start_date, formatter);
					
					BoundStatement statement = pstmt.bind()
									  .setUuid(0, UUID.randomUUID())
									  .setString(1, obj.title_description)
									  .setInstant(2, Instant.now())
									  .setDouble(3, obj.total_other_pay)
									  .setDouble(4, obj.regular_gross_paid)
									  .setString(5, obj.last_name);
//					System.out.println(statement);
					bstmt = bstmt.add(statement);
				}
				
				ResultSet rs = session.execute(bstmt);
				
//				bstmt = bstmt.clear();
				
				System.out.println(rs.wasApplied());

				System.out.println("Batch : " + j + " Inserted");
			}	
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		java.util.Date dEnd = new java.util.Date();
		long ldiff = dEnd.getTime()-dStart.getTime();
		System.out.print(MessageFormat.format("Insertion Time (ms): {0} #NumRows: {1}, Driver Type: {2} \n", ldiff, numRecords, "DataStax Java"));
		}		
}
