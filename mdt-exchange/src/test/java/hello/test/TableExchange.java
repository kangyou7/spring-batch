package hello.test;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.test.context.TestPropertySource;
import org.springframework.util.NumberUtils;

import hello.exchange.Utils;

@TestPropertySource("classpath:exchange.properties")
public class TableExchange {

	@Autowired
	Environment env;

//	@Test
//	public void forwardExchange() throws IOException {
//
//		
//		String day = "20151104";
//		//File f = new File("c:\\exchange\\"+day+".sql");
//		String[] tablenames = { "PCTR01","ITRF01","ITRF10","ITEF01","ITEF10","LOGD01","LOGD10","LOGD11","LOGD20","LOGD21","LOGD30","RLFR01","RLFR10","RLFR11","RLFR20","RLFR21","RLFR30","RCEF01","RCEF10","RCEF11","RCEF20","RCEF21","RCEF30","M1A201" };
//		for (String table : tablenames) {
//			for (int i = 0; i < 24; i++) {
//				String s = "ALTER TABLE LOCU.TB_T" + table + "Y" + " EXCHANGE PARTITION PT_T" + table + "Y_" + day
//						+ StringUtils.leftPad(String.valueOf(i), 2, "0") + " WITH TABLE LOCU.TB_T" + table + "X"
//						+ " WITHOUT VALIDATION;";
//				String t = "ALTER TABLE LOCU.TB_T" + table + "W" + " EXCHANGE PARTITION PT_T" + table + "W_" + day
//						+ StringUtils.leftPad(String.valueOf(i), 2, "0") + " WITH TABLE LOCU.TB_T" + table + "X"
//						+ " WITHOUT VALIDATION;";
//				
//				System.out.println(s);
//				System.out.println(t);
//				System.out.println("\n");
//				//FileUtils.write(f, s,true);
//				//FileUtils.write(f, t,true);
//			}
//			// ALTER TABLE LOCU.tb_titef01Y EXCHANGE PARTITION
//			// PT_TITEF01Y_2015110318 WITH TABLE LOCU.TB_TITEF01X WITHOUT
//			// VALIDATION;
//			// ALTER TABLE LOCU.tb_titef01W EXCHANGE PARTITION
//			// PT_TITEF01W_2015110318 WITH TABLE LOCU.TB_TITEF01X WITHOUT
//			// VALIDATION;
//		}
//	}
//
//	@Test
//	public void mergeTable() {
//		String day = "20151104";
//		String[] tablenames = { "ITEF01"};
//		for (String table : tablenames) {
//			for (int i = 23; i >= 1; i--) {
//				String z = "ALTER TABLE LOCU.TB_T" + table + "W" + " MERGE PARTITIONS " + "PT_T" + table + "W_" + day
//						+ StringUtils.leftPad(String.valueOf(i), 2, "0") + "," + "PT_T" + table + "W_" + day
//						+ StringUtils.leftPad(String.valueOf(i - 1), 2, "0") + " INTO PARTITION " + "PT_T" + table
//						+ "W_" + day + StringUtils.leftPad(String.valueOf(i-1), 2, "0");
//
//				// strSQL = "ALTER TABLE LOCU." + table_name + " MERGE
//				// PARTITIONS "
//				// + cPartition_v1 + ", " + cPartition_v2 + " INTO PARTITION " +
//				// cPartition_v2;
//
//				System.out.println(StringUtils.leftPad(String.valueOf(-(i-24)), 2, "0")+" "+z);
//			}
//		}
//	}
//	
//	@Test
//	public void moveTablesapce() {
//		String day ="20151105";
//		String tablespace ="tablespaceTest";
//		String[] tablenames ={"ITEF01","ITEF10"};
//		for (String table : tablenames) {
//				String z = "ALTER TABLE LOCU.TB_T"+table+"W"+"  MOVE PARTITION "+"PT_T"+table+"W_"+day+"00"+" TABLESPACE "+tablespace;
//				//"ALTER TABLE LOCU." + table_name + " MOVE PARTITION " + cPartition + " TABLESPACE "+ tableSpace;
//
//					System.out.println(z);
//
//		}
//	}
	
	@Test
	public void now() {
		System.out.println(Utils.now());
		System.out.println(NumberUtils.parseNumber(Utils.now(), Integer.class));
		System.out.println(Utils.oneHourBefore());
		String ddHH = Utils.oneHourBefore();
		System.out.println(ddHH.substring(0, 8));
		System.out.println(ddHH.substring(8));
		ddHH ="2015111001";
		System.out.printf("%d",NumberUtils.parseNumber(ddHH.substring(8),Integer.class));
	}

}
