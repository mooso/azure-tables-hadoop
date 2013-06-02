package com.microsoft.hadoop.azure;

import java.io.*;
import java.net.*;

import org.apache.hadoop.conf.Configuration;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.table.client.*;

/**
 * The configuration used for Azure Table jobs.
 */
public class AzureTableConfiguration {
	public static enum Keys {
		// The table name.
		TABLE_NAME("azure.table.name"),
		// The account URI.
		ACCOUNT_URI("azure.table.account.uri"),
		// The storage key.
		STORAGE_KEY("azure.table.storage.key"),
		// The class to use to partition the rows in an Azure Table
		PARTITIONER_CLASS("azure.table.partitioner.class");
	
		private final String key;
	
		Keys(String key) {
			this.key = key;
		}
	
		public String getKey() {
			return key;
		}
	}

	/**
	 * Configure the input table to be processed.
	 * @param conf The configuration for the job.
	 * @param tableName The name of the table.
	 * @param accountUri The fully qualified account URI, e.g. http://myacc.table.core.windows.net.
	 * @param storageKey The key for the Azure Storage account.
	 */
	public static void configureInputTable(Configuration conf,
			String tableName, URI accountUri, String storageKey) {
		conf.set(Keys.TABLE_NAME.getKey(), tableName);
		conf.set(Keys.ACCOUNT_URI.getKey(), accountUri.toString());
		conf.set(Keys.STORAGE_KEY.getKey(), storageKey);
	}

	public static String getTableName(Configuration conf) {
		return conf.get(Keys.TABLE_NAME.getKey());
	}

	public static AzureTablePartitioner getPartitioner(Configuration job)
			throws IOException {
		Class<? extends AzureTablePartitioner> partitionerClass =
				job.getClass(Keys.PARTITIONER_CLASS.getKey(),
					DefaultTablePartitioner.class,
					AzureTablePartitioner.class);
		try {
			return partitionerClass.newInstance();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public static CloudTable getTableReference(Configuration job)
			throws IOException {
		CloudTableClient tableClient = createTableClient(job);
		String tableName = getTableName(job);
		try {
			return tableClient.getTableReference(tableName);
		} catch (URISyntaxException e) {
			throw new IOException(e);
		} catch (StorageException e) {
			throw new IOException(e);
		}
	}

	public static CloudTableClient createTableClient(Configuration job)
			throws IOException {
		String accountUriString = job.get(Keys.ACCOUNT_URI.getKey());
		if (accountUriString == null) {
			throw new IllegalArgumentException(
					Keys.ACCOUNT_URI.getKey() + " not specified.");
		}
		String storageKey = job.get(Keys.STORAGE_KEY.getKey());
		if (storageKey == null) {
			throw new IllegalArgumentException(
					Keys.STORAGE_KEY.getKey() + " not specified.");
		}
		URI accountUri;
		try {
			accountUri = new URI(accountUriString);
		} catch (URISyntaxException ex) {
			throw new IllegalArgumentException(
					String.format("Invalid value specified for %s: %s",
							Keys.ACCOUNT_URI, accountUriString),
					ex);
		}
		String accountName = accountUri.getAuthority().split("\\.")[0];
		StorageCredentials creds =
			new StorageCredentialsAccountAndKey(accountName, storageKey);
		return new CloudTableClient(accountUri, creds);
	}
}
