package com.idesparado.hadoop.hive;

import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.apache.hive.hcatalog.streaming.StreamingConnection;
import org.apache.hive.hcatalog.streaming.StrictJsonWriter;
import org.apache.hive.hcatalog.streaming.TransactionBatch;

public class HiveStreamingExample {
	private static Random rnd;

	static {
		rnd = new Random(System.nanoTime());
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 5) {
			printUsage();
			System.exit(1);
		}

		String metastore = args[0];
		String database = args[1];
		String table = args[2];
		int count = Integer.valueOf(args[3]);
		int delay = Integer.valueOf(args[4]);

		HiveEndPoint hiveEP = new HiveEndPoint(String.format("thrift://%s", metastore), database, table, new ArrayList<String>());
		HiveConf hiveConf = createHiveStreamingConf(metastore);

		StreamingConnection connection = hiveEP.newConnection(true);
		StrictJsonWriter jsonWriter = new StrictJsonWriter(hiveEP, hiveConf);
		TransactionBatch txnBatch = connection.fetchTransactionBatch(5, jsonWriter);

		txnBatch.beginNextTransaction();
		for (int i = 0; i < count; ++i) {
			if (((i % 100) == 0) && (i != 0)) {
				txnBatch.commit();
				txnBatch.beginNextTransaction();
			}
			txnBatch.write(createRandomJson().getBytes());
			Thread.sleep(delay);
		}
		txnBatch.commit();

		txnBatch.close();
		connection.close();
	}

	private static HiveConf createHiveStreamingConf(String metastore) {
		HiveConf hiveConf = new HiveConf();

		hiveConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		hiveConf.set("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
		hiveConf.set("hive.compactor.initiator.on", "true");
		hiveConf.set("hive.compactor.worker.threads", "5");
		hiveConf.set("hive.support.concurrency", "true");
		hiveConf.set("hive.in.test", "true");
		hiveConf.set("hive.metastore.uris", String.format("thrift://%s", metastore));

		return hiveConf;
	}

	private static String createRandomJson() {
		return String.format("{\"num\":%d, \"name\":\"%s\"}", rnd.nextInt(1000), UUID.randomUUID().toString());
	}

	private static void printUsage() {
		System.err.println("Usage : " + HiveStreamingExample.class.getName() + " <metastore> <database> <table> <count> <delay>");
	}
}
