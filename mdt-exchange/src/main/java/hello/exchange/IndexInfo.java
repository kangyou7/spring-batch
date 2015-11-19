package hello.exchange;

public class IndexInfo {
	private String indexOwner, indexName, partitionName, tablespaceName;

	public IndexInfo(String indexOwner, String indexName, String partitionName, String tablespaceName) {
		super();
		this.indexOwner = indexOwner;
		this.indexName = indexName;
		this.partitionName = partitionName;
		this.tablespaceName = tablespaceName;
	}

	public IndexInfo() {
	}

	public String getIndexOwner() {
		return indexOwner;
	}

	public void setIndexOwner(String indexOwner) {
		this.indexOwner = indexOwner;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public String getPartitionName() {
		return partitionName;
	}

	public void setPartitionName(String partitionName) {
		this.partitionName = partitionName;
	}

	public String getTablespaceName() {
		return tablespaceName;
	}

	public void setTablespaceName(String tablespaceName) {
		this.tablespaceName = tablespaceName;
	}

	@Override
	public String toString() {
		return "IndexInfo [indexOwner=" + indexOwner + ", indexName=" + indexName + ", partitionName=" + partitionName
				+ ", tablespaceName=" + tablespaceName + "]";
	}
}
