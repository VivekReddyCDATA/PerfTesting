import java.lang.reflect.Field;
		
public class PayRollCompressed{
	
	public String last_name;
	public String title_description;
	public Double regular_gross_paid;
	public Double total_other_pay;
	public String agency_start_date;
	
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