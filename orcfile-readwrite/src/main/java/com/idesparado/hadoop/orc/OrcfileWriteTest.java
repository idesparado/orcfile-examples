package com.idesparado.hadoop.orc;

import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterOptions;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

public class OrcfileWriteTest {
	private static Random rnd;

	static {
		rnd = new Random(System.nanoTime());
	}

	public static class Student {
		int num;
		String name;
		String pdate;

		public Student(int num, String name, String pdate) {
			this.num = num;
			this.name = name;
			this.pdate = pdate;
		}

		@Override
		public String toString() {
			return String.format(" num : %d, name : %s, pdate : %s", name, num, pdate);
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			printUsage();
			System.exit(1);
		}

		String filename = args[0];
		int count = Integer.valueOf(args[1]);

		writeDataToOrcfile(filename, count);
	}

	private static void writeDataToOrcfile(String filename, int count) throws Exception {
		Configuration conf = new Configuration();

		Path path = new Path(filename);
		WriterOptions opts = OrcFile.writerOptions(conf);
		ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(Student.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		opts.inspector(inspector);

		Writer writer = OrcFile.createWriter(path, opts);

		for (int i = 0; i < count; ++i) {
			writer.addRow(new Student(rnd.nextInt(1000), UUID.randomUUID().toString(), "20160101"));
		}

		writer.close();
	}

	private static void printUsage() {
		System.out.println(String.format("Usage : %s <filename> <row count>", OrcfileWriteTest.class.getName()));
	}
}
