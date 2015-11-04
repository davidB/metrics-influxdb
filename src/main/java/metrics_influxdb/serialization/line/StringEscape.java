package metrics_influxdb.serialization.line;

public class StringEscape {

	public static String escape(String name, char ... toEscape) {
		String result = name;
		
		for (char c : toEscape) {
			result = result.replace(Character.toString(c), "\\" + c);
		}
		
		return result;
	}

}
