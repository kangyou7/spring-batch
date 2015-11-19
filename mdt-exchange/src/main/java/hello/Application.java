package hello;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.NumberUtils;

import com.jolbox.bonecp.BoneCPDataSource;

import hello.exchange.TableAdjust;
import hello.exchange.Utils;
import hello.security.AesCrypto;

@SpringBootApplication
@PropertySource("file:${location}/exchange.properties")
public class Application implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(Application.class);

	private static final String EXCHANGE = "ec";
	private static final String MERGE = "me";
	private static final String REBUILD_INDEX = "ri";
	private static final String NOW = "now";
	private static final String YESTERDAY = "yesterday";

	public static void main(String args[]) {

		ApplicationContext ctx = SpringApplication.run(Application.class, args);

		log.debug("Let's inspect the beans provided by Spring Boot:");

		String[] beanNames = ctx.getBeanDefinitionNames();
		Arrays.sort(beanNames);
		for (String beanName : beanNames) {
			log.debug(beanName);
		}
	}

	@Autowired
	ApplicationContext ctx;

	@Override
	public void run(String... strings) throws Exception {

		for (int i = 0; i < strings.length; i++) {
			System.out.println((i + 1) + "번째 매개변수:" + strings[i]);
		}

		TableAdjust ta = ctx.getBean(TableAdjust.class);

		if (strings.length != 2) {
			System.out.println("\n======================================");
			System.out.println("2개의 매개변수가 필요합니다.(Need parameter)");
			System.out.println("<<사용법>>");
			System.out.println(
					"exchange실행(Excute exchange)==> mdt_exchange ec now/yesterday/일시(YYYYMMDDHH)/일자(YYYYMMDD)");
			System.out.println("[옵션:ec  now] 현재 시간(hour)기준으로 1시간 전 시간(hour)로 exchange실행");
			System.out.println("[옵션:ec  yesterday] 전일자(YYYYMMDD)으로 exchange실행");
			System.out.println("[옵션:ec  일시] 지정일시(YYYYMMDDHH)으로 exchange실행");
			System.out.println("[옵션:ec  일자] 지정일자(YYYYMMDD)으로 exchange실행");
			System.out.println("\n");
			System.out.println("merge실행(Excute merge)==> mdt_exchange me yesterday/일자(YYYYMMDD)");
			System.out.println("[옵션:me yesterday]전일자(YYYYMMDD)으로 merge실행");
			System.out.println("[옵션:me 일자] 지정일자(YYYYMMDD)으로 merge실행");
			System.out.println("\n");
			System.out.println(
					"rebuildIndex실행(Excute rebuildIndex)==> mdt_exchange ri yesterday/일시(YYYYMMDDHH)/일자(YYYYMMDD)");
			System.out.println("[옵션:ri yesterday]전일자(YYYYMMDD)으로 rebuildIndex실행");
			System.out.println("[옵션:ri 일시] 지정일시(YYYYMMDDHH)으로 rebuildIndex실행");
			System.out.println("[옵션:ri 일자] 지정일자(YYYYMMDD)으로 rebuildIndex실행");
			System.out.println("======================================\n");
			System.exit(1);
		}

		if (strings[0].equalsIgnoreCase(EXCHANGE)) {
			if (NOW.equalsIgnoreCase(strings[1])) {
				String[] tables = ta.getTableNames();
				String ddHH = Utils.oneHourBefore();
				for (String table : tables) {
					ta.exchangeYtable(table, ddHH.substring(0,8), NumberUtils.parseNumber(ddHH.substring(8),Integer.class));
					ta.exchangeWtable(table, ddHH.substring(0,8), NumberUtils.parseNumber(ddHH.substring(8),Integer.class));
				}
				
				ta.rebuildIndex(ddHH.substring(0,8),ddHH.substring(8));
			}

			if (YESTERDAY.equalsIgnoreCase(strings[1])) {
				ta.exchangeForward(Utils.yesterday());
				ta.rebuildIndex(Utils.yesterday(),StringUtils.EMPTY);
			}

			if (strings[1].length() == 8) {
				if (Utils.isThisDateValid(strings[1], "yyyymmdd")) {
					ta.exchangeForward(strings[1]);
					ta.rebuildIndex(strings[1],StringUtils.EMPTY);
				} else {
					System.out.println("올바른 일자가 아닙니다.");
					System.exit(1);
				}
			}

			if (strings[1].length() == 10) {
				if (Utils.isThisDateValid(strings[1], "yyyymmddHH")) {
					String dd = strings[1].substring(0, 8);
					int hh = NumberUtils.parseNumber(strings[1].substring(8), Integer.class);
					String[] tables = ta.getTableNames();
					for (String table : tables) {
						ta.exchangeYtable(table, dd, hh);
						ta.exchangeWtable(table, dd, hh);
					}
					ta.rebuildIndex(dd,StringUtils.leftPad(String.valueOf(hh),2,"0"));
				} else {
					System.out.println("올바른 일시가 아닙니다.");
					System.exit(1);
				}
			}

		}

		if (MERGE.equalsIgnoreCase(strings[0])) {
			if (YESTERDAY.equalsIgnoreCase(strings[1])) {
				ta.mergeTable(Utils.yesterday());
				ta.moveTablespace(Utils.yesterday());
				ta.rebuildIndex(Utils.yesterday(), StringUtils.EMPTY);
			}

			if (strings[1].length() == 8) {
				if (Utils.isThisDateValid(strings[1], "yyyymmdd")) {
					ta.mergeTable(strings[1]);
					ta.moveTablespace(strings[1]);
					ta.rebuildIndex(strings[1], StringUtils.EMPTY);
				} else {
					System.out.println("올바른 일자가 아닙니다.");
					System.exit(1);
				}

			}
		}

		if (REBUILD_INDEX.equalsIgnoreCase(strings[0])) {
			if (YESTERDAY.equalsIgnoreCase(strings[1])) {
				ta.rebuildIndex(Utils.yesterday(), StringUtils.EMPTY);
			}

			if (strings[1].length() == 8) {
				if (Utils.isThisDateValid(strings[1], "yyyymmdd")) {
					ta.rebuildIndex(strings[1], StringUtils.EMPTY);
				} else {
					System.out.println("올바른 일자가 아닙니다.");
					System.exit(1);
				}

			}

			if (strings[1].length() == 10) {
				if (Utils.isThisDateValid(strings[1], "yyyymmddHH")) {
					String dd = strings[1].substring(0, 8);
					String hh = strings[1].substring(8);
					ta.rebuildIndex(dd, hh);
				} else {
					System.out.println("올바른 일시가 아닙니다.");
					System.exit(1);
				}
			}
		}

	}

	@Autowired
	Environment env;

	@Bean
	public JdbcTemplate mdts() throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException,
			IllegalBlockSizeException, BadPaddingException {
		return new JdbcTemplate(mdtsSource());
	}

	@Bean
	public BoneCPDataSource mdtsSource() throws InvalidKeyException, NoSuchAlgorithmException, NoSuchPaddingException,
			IllegalBlockSizeException, BadPaddingException {
		BoneCPDataSource b = new BoneCPDataSource();
		b.setDriverClass(env.getProperty("oracle.driver"));
		b.setJdbcUrl(env.getProperty("oracle.url"));
		b.setUsername(env.getProperty("oracle.username"));
		b.setPassword(AesCrypto.getDecode(env.getProperty("oracle.password")));
		return b;
	}

}