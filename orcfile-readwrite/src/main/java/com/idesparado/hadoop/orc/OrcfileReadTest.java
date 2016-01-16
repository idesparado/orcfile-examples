package com.idesparado.hadoop.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.ReaderOptions;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;

public class OrcfileReadTest {
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			printUsage();
			System.exit(1);
		}
		String filename = args[0];

		readDataFromOrcfile(filename);
	}

	private static void readDataFromOrcfile(String filename) throws Exception {
		Configuration conf = new Configuration();

		Path path = new Path(filename);
		ReaderOptions opts = OrcFile.readerOptions(conf);

		Reader reader = OrcFile.createReader(path, opts);

		long count = reader.getNumberOfRows();
		System.out.println(String.format("Rows : %d", count));
		System.out.println(reader.getObjectInspector().getTypeName());
		RecordReader rows = reader.rows();

		int n = 0;
		while (rows.hasNext()) {
			if (++n > 5) {
				break;
			}
			Object row = rows.next(null);
			System.out.println((OrcStruct) row);
		}
	}

	private static void printUsage() {
		System.out.println(String.format("Usage : %s <filename>", OrcfileReadTest.class.getName()));
	}
}
