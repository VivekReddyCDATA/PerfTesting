

import java.sql.*;
import java.io.*;
import java.lang.reflect.Field;
import java.util.*; 
import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Random; 
import java.util.concurrent.TimeUnit;
import java.util.UUID;

import org.json.simple.JSONArray; 
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DataLoader {
	
	public static ConnectDB SQLObj;
	
	public DataLoader(){
			
	}
	
	public DataLoader(String driverType){
		long ldiff; 
		java.util.Date dStart = new java.util.Date(); 
		SQLObj.connect();
		java.util.Date dEnd = new java.util.Date();
	    ldiff = dEnd.getTime()-dStart.getTime();
	    System.out.print(MessageFormat.format("Database connection time (ms): {0}, Driver Type: {1}\n", ldiff, driverType));	
	}
		
	public void DOMInsert(String path) {
		
		ObjectMapper mapper = new ObjectMapper();
		PreparedStatement pstmt;
		PayRoll[] payrollObjects = null;
		
		String Query =  "INSERT INTO Cdata.test.nyc_payroll (fiscal_year, payroll_number, agency_name, last_name, "
						+ "first_name,  mid_init, agency_start_date, work_location_borough, title_description, " 
						+ " leave_status_as_of_july_31, base_salary, pay_basis, regular_hours, regular_gross_paid, "
						+ "	ot_hours, total_ot_paid, total_other_pay, id)"
						+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		
		try {		
			payrollObjects = mapper.readValue(new File(path), PayRoll[].class);      
        } 
		catch (IOException e) {
            e.printStackTrace();
        }
		
		int numRecords = payrollObjects.length;
		
		int numRecordsPerUpdate = 100;
		int UpdateCount = numRecords/numRecordsPerUpdate;
		
		assert UpdateCount*numRecordsPerUpdate == numRecords : " Non Divisible by " + 1000 + " Error"; 
		
		java.util.Date dStart = new java.util.Date();
		if (payrollObjects != null) {
			try {
				SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss.SSS");
				pstmt = SQLObj.getPreparedStatement(Query);
				
		
				for (int j = 0; j < UpdateCount; j++) {
					for (int k = 0; k < numRecordsPerUpdate; k++) {
						
						int i = j*numRecordsPerUpdate + k;
						
						PayRoll obj = payrollObjects[i];
						pstmt.setInt(1, obj.fiscal_year);
						pstmt.setInt(2, obj.payroll_number);
						pstmt.setString(3, obj.agency_name);
						pstmt.setString(4, obj.last_name);
						pstmt.setString(5, obj.first_name);
						pstmt.setString(6, obj.mid_init);
						pstmt.setTimestamp(7, new java.sql.Timestamp(dateFormat.parse(obj.agency_start_date).getTime()));
						pstmt.setString(8, obj.work_location_borough);
						pstmt.setString(9, obj.title_description);
						pstmt.setString(10, obj.leave_status_as_of_july_31);
						pstmt.setDouble(11, obj.base_salary);
						pstmt.setString(12, obj.pay_basis);
						pstmt.setDouble(13, obj.regular_hours);
						pstmt.setDouble(14, obj.regular_gross_paid);
						pstmt.setDouble(15, obj.ot_hours);
						pstmt.setDouble(16, obj.total_ot_paid);
						pstmt.setDouble(17, obj.total_other_pay);
						pstmt.setString(18,  UUID.randomUUID().toString());
						pstmt.addBatch();
					}
					
					int[] r = pstmt.executeBatch();
					System.out.println("Batch : " + j + " Inserted");
				}	
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		else {
			System.out.println("No Data Has been Extracted from JSON File");
		}
		java.util.Date dEnd = new java.util.Date();
		long ldiff = dEnd.getTime()-dStart.getTime();
		System.out.print(MessageFormat.format("Insertion Time (ms): {0} #NumRows: {1}, Driver Type: {2} \n", ldiff, numRecords, "CData"));
	}	
	
	public static void main(String[] args) throws Exception
    {	
		SQLObj = new CDATACassandraDriver();
		DataLoader DL = new DataLoader("CData");
		DL.DOMInsert("/home/vivekreddy/Desktop/Intern/nyc_payroll_900k.json");
		SQLObj.terminate();	
    }
}


class PayRoll{
	
	public int fiscal_year;
	public int payroll_number;
	public String agency_name;
	public String last_name;
	public String first_name;
	public String mid_init;
	public String agency_start_date;
	public String work_location_borough;
	public String title_description;
	public String leave_status_as_of_july_31;
	public Double base_salary;
	public String pay_basis;
	public Double regular_hours;
	public Double regular_gross_paid;
	public Double ot_hours;
	public Double total_ot_paid;
	public Double total_other_pay;
	
	public void display() {
		for (Field field : this.getClass().getDeclaredFields()) {
		    field.setAccessible(true);
		    String name = field.getName();
		    try {
		    	 Object value = field.get(this);
		    	 System.out.printf("%s: %s%n", name, value);
		    }
		    catch (Exception e) {
		    	System.out.println("Illegal Access: " + name);
		    }   
		}
	}
}