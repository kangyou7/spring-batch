package hello.exchange;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

@Component
public class TableAdjust {

	private static final Logger log = LoggerFactory.getLogger(TableAdjust.class);

	@Autowired
	Environment env;

	@Autowired
	JdbcTemplate mdts;

	public void exchangeForward(String day) {
		String method = new Object() {
		}.getClass().getEnclosingMethod().getName();

		log.info("exchangeForward start.....");
		String[] tablenames = getTableNames();
		for (String table : tablenames) {
			for (int i = 0; i < 24; i++) {
				String y = "ALTER TABLE LOCU.TB_T" + table + "Y" + " EXCHANGE PARTITION PT_T" + table + "Y_" + day
						+ StringUtils.leftPad(String.valueOf(i), 2, "0") + " WITH TABLE LOCU.TB_T" + table + "X"
						+ " WITHOUT VALIDATION";
				String w = "ALTER TABLE LOCU.TB_T" + table + "W" + " EXCHANGE PARTITION PT_T" + table + "W_" + day
						+ StringUtils.leftPad(String.valueOf(i), 2, "0") + " WITH TABLE LOCU.TB_T" + table + "X"
						+ " WITHOUT VALIDATION";

				try {
					log.info("Y table exchange:\n" + y);
					mdts.execute(y);
				} catch (Exception e) {
					log.info(method + ">" + e.getMessage());
				}

				try {
					log.info("W table exchange:\n" + w);
					mdts.execute(w);
				} catch (Exception e) {
					log.info(method + ">" + e.getMessage());
				}

			}

		}
		log.info("exchangeForward end.....");
	}

	public void exchangeBackward(String day) {
		log.info("exchangeBackward start.....");
		String[] tablenames = getTableNames();
		for (String table : tablenames) {
			for (int i = 0; i < 24; i++) {
				String y = "ALTER TABLE LOCU.TB_T" + table + "Y" + " EXCHANGE PARTITION PT_T" + table + "Y_" + day
						+ StringUtils.leftPad(String.valueOf(i), 2, "0") + " WITH TABLE LOCU.TB_T" + table + "X"
						+ " WITHOUT VALIDATION";
				String w = "ALTER TABLE LOCU.TB_T" + table + "W" + " EXCHANGE PARTITION PT_T" + table + "W_" + day
						+ StringUtils.leftPad(String.valueOf(i), 2, "0") + " WITH TABLE LOCU.TB_T" + table + "X"
						+ " WITHOUT VALIDATION";

				try {
					log.info("W table exchange:\n" + w);
					mdts.execute(w);
				} catch (Exception e) {
					log.error(e.getMessage());
				}

				try {
					log.info("Y table exchange:\n" + y);
					mdts.execute(y);
				} catch (Exception e) {
					log.error(e.getMessage());
				}

			}

		}
		log.info("exchangeBackward end.....");

	}

	public void mergeTable(String day) {
		String method = new Object() {
		}.getClass().getEnclosingMethod().getName();

		log.info("mergeTable start.....");
		String[] tablenames = getTableNames();
		for (String table : tablenames) {
			try {
				for (int i = 0; i <= 23; i = i + 2) {
					mergeWtable(table, day, i, i + 1);
				}

				for (int i = 1; i <= 23; i = i + 4) {
					mergeWtable(table, day, i, i + 2);
				}

				for (int i = 3; i <= 23; i = i + 8) {
					mergeWtable(table, day, i, i + 4);
				}

				for (int i = 7; i <= 15; i = i + 8) {
					mergeWtable(table, day, i, i + 8);
				}
			} catch (Exception e) {
				log.info(method + ">" + e.getMessage());
			}

		}
		log.info("mergeTable end.....");
	}

	public void moveTablespace(String day) {
		String method = new Object() {
		}.getClass().getEnclosingMethod().getName();

		log.info("moveTablespace start.....");
		String tablespace = getTablespace();
		String[] tablenames = getTableNames();
		for (String table : tablenames) {
			String z = "ALTER TABLE LOCU.TB_T" + table + "W" + "  MOVE PARTITION " + "PT_T" + table + "W_" + day + "23"
					+ " TABLESPACE " + tablespace;
			try {
				log.info(z);
				mdts.execute(z);
			} catch (Exception e) {
				log.info(method + ">" + e.getMessage());
			}
		}
		log.info("moveTablespace end.....");

	}

	public void rebuildIndex(String day, String hh) {
		String method = new Object() {
		}.getClass().getEnclosingMethod().getName();

		log.info("rebuildIndex start.....");
		List<IndexInfo> list = new ArrayList<IndexInfo>();
		if (StringUtils.EMPTY.equals(hh)) {
			list = getIndexInfo(day, StringUtils.EMPTY);
		} else {
			list = getIndexInfo(day, hh);
		}

		for (IndexInfo ii : list) {
			String partition = ii.getPartitionName().equals("NULL") ? "" : ii.getPartitionName();
			String z = "CALL " + ii.getIndexOwner() + ".SP_T_IDX_REBUILD('" + ii.getIndexOwner() + "','"
					+ ii.getIndexName() + "','" + partition + "','" + ii.getTablespaceName() + "')";
			try {
				log.info(z);
				mdts.execute(z);
			} catch (Exception e) {
				log.info(method + ">" + e.getMessage());
			}
		}
		log.info("rebuildIndex end.....");
	}

	public List<IndexInfo> getIndexInfo(String day, String hh) {
		String method = new Object() {
		}.getClass().getEnclosingMethod().getName();

		String sql;

		if (StringUtils.EMPTY.equals(hh)) {
			sql = "select owner index_owner,index_name,'NULL' partition_name,tablespace_name \n" + "from ALL_indexes \n"
					+ "where table_name like 'TB_T%X' \n" + "union all \n" + "SELECT index_owner, \n"
					+ "  index_name, \n" + "  partition_name, \n" + "  tablespace_name \n"
					+ "FROM ALL_IND_PARTITIONS \n" + "WHERE partition_name LIKE 'PT_T%_' \n" + "  ||" + day + "\n"
					+ "  ||'%' \n";
		} else {
			sql = "select owner index_owner,index_name,'NULL' partition_name,tablespace_name \n" + "from ALL_indexes \n"
					+ "where table_name like 'TB_T%X' \n" + "union all \n" + "SELECT index_owner, \n"
					+ "  index_name, \n" + "  partition_name, \n" + "  tablespace_name \n"
					+ "FROM ALL_IND_PARTITIONS \n" + "WHERE partition_name LIKE 'PT_T%_' \n" + "  ||" + day + hh + "\n";
		}

		List<IndexInfo> list = new ArrayList<IndexInfo>();

		try {
			log.info(sql);
			list = mdts.query(sql, new RowMapper<IndexInfo>() {

				@Override
				public IndexInfo mapRow(ResultSet rs, int rowNum) throws SQLException {
					IndexInfo i = new IndexInfo();
					i.setIndexName(rs.getString("index_name"));
					i.setIndexOwner(rs.getString("index_owner"));
					i.setPartitionName(rs.getString("partition_name"));
					i.setTablespaceName(rs.getString("tablespace_name"));
					return i;
				}
			});

			log.info("rebulid target count:" + list.size());
		} catch (Exception e) {
			log.info(method + ">" + e.getMessage());
		}

		return list;
	}

	public String getTablespace() {
		String method = new Object() {
		}.getClass().getEnclosingMethod().getName();

		final String GET_TABLESPACE = "SELECT TABLESPACE_NAME \n" + "FROM \n" + "  (SELECT ROWNUM NO, \n"
				+ "    TABLESPACE_NAME \n" + "  FROM \n" + "    ( SELECT DISTINCT TABLESPACE_NAME \n"
				+ "    FROM ALL_TAB_PARTITIONS \n" + "    WHERE TABLE_NAME = 'TB_TITEF01W' \n"
				+ "    ORDER BY TABLESPACE_NAME \n" + "    ) \n" + "  ) \n" + "WHERE NO = \n"
				+ "  (SELECT (TO_NUMBER(TO_CHAR(SYSDATE,'DD')) - (CNT*TRUNC(TO_NUMBER(TO_CHAR(SYSDATE,'DD')) / CNT ))) + 1 \n"
				+ "  FROM \n" + "    (SELECT COUNT(DISTINCT TABLESPACE_NAME) CNT \n" + "    FROM ALL_TAB_PARTITIONS \n"
				+ "    WHERE TABLE_NAME = 'TB_TITEF01W' \n" + "    ) \n" + "  ) \n";

		String tablespace = "";
		try {
			log.info(GET_TABLESPACE);
			tablespace = mdts.queryForObject(GET_TABLESPACE, String.class);
		} catch (Exception e) {
			log.info(method + ">" + e.getMessage());
		}
		return tablespace;
	}

	public String[] getTableNames() {
		String p = env.getProperty("table");
		return StringUtils.split(p, ",");
	}

	public void exchangeYtable(String table, String day, int hour) {
		String method = new Object() {
		}.getClass().getEnclosingMethod().getName();

		String y = "ALTER TABLE LOCU.TB_T" + table + "Y" + " EXCHANGE PARTITION PT_T" + table + "Y_" + day
				+ StringUtils.leftPad(String.valueOf(hour), 2, "0") + " WITH TABLE LOCU.TB_T" + table + "X"
				+ " WITHOUT VALIDATION";

		try {
			log.info("Y table exchange:\n" + y);
			mdts.execute(y);
		} catch (Exception e) {
			log.info(method + ">" + e.getMessage());
		}

	}

	public void exchangeWtable(String table, String day, int hour) {
		String method = new Object() {
		}.getClass().getEnclosingMethod().getName();

		String w = "ALTER TABLE LOCU.TB_T" + table + "W" + " EXCHANGE PARTITION PT_T" + table + "W_" + day
				+ StringUtils.leftPad(String.valueOf(hour), 2, "0") + " WITH TABLE LOCU.TB_T" + table + "X"
				+ " WITHOUT VALIDATION";
		try {
			log.info("W table exchange:\n" + w);
			mdts.execute(w);
		} catch (Exception e) {
			log.info(method + ">" + e.getMessage());
		}

	}

	public void mergeWtable(String table, String day, int src, int dest) throws Exception {

		String z = "ALTER TABLE LOCU.TB_T" + table + "W" + " MERGE PARTITIONS " + "PT_T" + table + "W_" + day
				+ StringUtils.leftPad(String.valueOf(src), 2, "0") + "," + "PT_T" + table + "W_" + day
				+ StringUtils.leftPad(String.valueOf(dest), 2, "0") + " INTO PARTITION " + "PT_T" + table + "W_" + day
				+ StringUtils.leftPad(String.valueOf(dest), 2, "0");

		log.info(z);
		mdts.execute(z);
	}

	public void moveTablesapce(String table, String day, String tablespace) throws Exception {

		String z = "ALTER TABLE LOCU.TB_T" + table + "W" + "  MOVE PARTITION " + "PT_T" + table + "W_" + day + "23"
				+ " TABLESPACE " + tablespace;
		log.info(z);
		mdts.execute(z);

	}

}
