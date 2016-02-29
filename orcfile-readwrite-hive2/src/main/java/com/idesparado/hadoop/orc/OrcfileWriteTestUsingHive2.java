package com.idesparado.hadoop.orc;

import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.WriterOptions;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

public class OrcfileWriteTestUsingHive2 {
	private static Random rnd;

	static {
		rnd = new Random(System.nanoTime());
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

		TypeDescription schema = TypeDescription.createStruct().addField("num", TypeDescription.createInt()) //
				.addField("name", TypeDescription.createString()) //
				.addField("pdate", TypeDescription.createString());

		WriterOptions opts = OrcFile.writerOptions(conf).setSchema(schema);
		Writer writer = OrcFile.createWriter(path, opts);
		VectorizedRowBatch batch = schema.createRowBatch(count);

		// createRowBatch(size) function has some bugs.
		batch.cols[0] = new LongColumnVector(count);
		batch.cols[1] = new BytesColumnVector(count);
		batch.cols[2] = new BytesColumnVector(count);
		batch.size = count;
		//

		LongColumnVector field0 = (LongColumnVector) batch.cols[0];
		BytesColumnVector field1 = (BytesColumnVector) batch.cols[1];
		BytesColumnVector field2 = (BytesColumnVector) batch.cols[2];

		for (int i = 0; i < count; ++i) {
			System.out.println(i);
			field0.vector[i] = rnd.nextInt(1000);
			field1.setVal(i, UUID.randomUUID().toString().getBytes());
			field2.setVal(i, "20160228".getBytes());
		}

		writer.addRowBatch(batch);
		writer.close();
		//// WriterOptions opts = OrcFile.writerOptions(conf);
		//// ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector(Student.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
		//// opts.inspector(inspector);
		////
		//// Writer writer = OrcFile.createWriter(path, opts);
		////
		//// for (int i = 0; i < count; ++i) {
		//// writer.addRow(new Student(rnd.nextInt(1000), UUID.randomUUID().toString(), "20160101"));
		//// }
		//
		// writer.close();
	}

	private static void printUsage() {
		System.out.println(String.format("Usage : %s <filename> <row count>", OrcfileWriteTestUsingHive2.class.getName()));
	}
}
