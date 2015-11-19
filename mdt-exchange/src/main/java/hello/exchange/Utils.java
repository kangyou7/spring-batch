package hello.exchange;

import java.sql.Date;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

import org.apache.commons.lang.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Utils {

	private static final Logger log = LoggerFactory.getLogger(Utils.class);

	public static String dateTime() {
		long time = System.currentTimeMillis();
		SimpleDateFormat dayTime = new SimpleDateFormat("yyyyMMddHHmmss");
		String strDT = dayTime.format(new Date(time));
		log.debug(strDT);
		return strDT;
	}

	public static String dateDD() {
		long time = System.currentTimeMillis();
		SimpleDateFormat dayTime = new SimpleDateFormat("yyyyMMdd");
		String strDT = dayTime.format(new Date(time));
		log.debug(strDT);
		return strDT;
	}

	public static String yesterday() {
		Format dateFormat = FastDateFormat.getInstance("yyyyMMdd", Locale.getDefault());
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -1);
		return dateFormat.format(cal.getTime());
	}

	public static String today() {
		Format dateFormat = FastDateFormat.getInstance("yyyyMMdd", Locale.getDefault());
		Calendar cal = Calendar.getInstance();
		return dateFormat.format(cal.getTime());

	}
	
	public static String now() {
		Format dateFormat = FastDateFormat.getInstance("HH", Locale.getDefault());
		Calendar cal = Calendar.getInstance();
		return dateFormat.format(cal.getTime());

	}
	
	public static String oneHourBefore() {
		Format dateFormat = FastDateFormat.getInstance("yyyyMMddHH", Locale.getDefault());
		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.HOUR, -1);
		return dateFormat.format(cal.getTime());
	}


	public static String timeStamp() {
		long time = System.currentTimeMillis();
		SimpleDateFormat dayTime = new SimpleDateFormat("yyyyMMddHHmmssSSS");
		String strDT = dayTime.format(new Date(time));
		log.debug(strDT);
		return strDT;
	}

	public static boolean isThisDateValid(String dateToValidate, String dateFromat) {

		if (dateToValidate == null) {
			return false;
		}

		SimpleDateFormat sdf = new SimpleDateFormat(dateFromat);
		sdf.setLenient(false);

		try {

			// if not valid, it will throw ParseException
			@SuppressWarnings("unused")
			java.util.Date date =  sdf.parse(dateToValidate);

		} catch (ParseException e) {

			e.printStackTrace();
			return false;
		}

		return true;
	}
}
