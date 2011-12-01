package ivory.lsh;

import static org.junit.Assert.assertTrue;
import ivory.lsh.data.FloatAsBytesWritable;
import ivory.lsh.projection.WriteRandomVectors;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.array.ArrayListOfFloatsWritable;
import edu.umd.cloud9.io.pair.PairOfWritables;

public class WriteRandomVectorsTest {

		public static void main(String[] args) throws IOException{
			SequenceFile.Writer writer1 = SequenceFile.createWriter(FileSystem.get(new Configuration()), new Configuration(), 
					new Path("file1"), IntWritable.class, ArrayListOfFloatsWritable.class);
			SequenceFile.Writer writer2 = SequenceFile.createWriter(FileSystem.get(new Configuration()), new Configuration(), 
					new Path("file2"), IntWritable.class, FloatAsBytesWritable.class);

			ArrayListOfFloatsWritable a1 = WriteRandomVectors.generateUnitRandomVector(100);
			FloatAsBytesWritable a2 = WriteRandomVectors.generateUnitRandomVectorAsBytes(100);

			writer1.append(new IntWritable(1), a1);
			writer1.close();

			writer2.append(new IntWritable(1), a2);
			writer2.close();

			List<PairOfWritables<WritableComparable, Writable>> listOfKeysPairs2 = SequenceFileUtils
			.readFile(new Path("file2"));
			FileSystem.get(new Configuration()).delete(new Path("file2"), true);
			List<PairOfWritables<WritableComparable, Writable>> listOfKeysPairs1 = SequenceFileUtils
			.readFile(new Path("file1"));
			FileSystem.get(new Configuration()).delete(new Path("file1"), true);

			FloatAsBytesWritable b2 = (FloatAsBytesWritable) listOfKeysPairs2.get(0).getRightElement();
			ArrayListOfFloatsWritable b1 = (ArrayListOfFloatsWritable) listOfKeysPairs1.get(0).getRightElement();

			for(int i=0;i<100;i++){
				float f = b1.get(i);
				float g = a1.get(i);
				assertTrue(f==g);
			}
			for(int i=0;i<100;i++){
				float f2 = b2.getAsFloat(i);
				float g2 = a2.getAsFloat(i);
				assertTrue(f2==g2);
			}


		}

	}