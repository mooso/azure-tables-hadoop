package com.microsoft.hadoop.azure;

/**
 * The configuration keys used for Azure jobs.
 */
public enum AzureTableConfiguration {
	// The table name.
	TABLE_NAME("azure.table.name"),
	// The account URI.
	ACCOUNT_URI("azure.table.account.uri"),
	// The storage key.
	STORAGE_KEY("azure.table.storage.key"),
	// The class to use to partition the rows in an Azure Table
	PARTITIONER_CLASS("azure.table.partitioner.class");

	private final String key;

	AzureTableConfiguration(String key) {
		this.key = key;
	}

	public String getKey() {
		return key;
	}
}
