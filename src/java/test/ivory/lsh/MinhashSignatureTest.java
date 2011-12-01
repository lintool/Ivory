package ivory.lsh;
import static org.junit.Assert.assertTrue;
import ivory.lsh.data.MinhashSignature;
import ivory.lsh.data.PermutationByBit;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.pair.PairOfWritables;


public class MinhashSignatureTest {
	static int D=20, vocabSize = 1000;
	
	private MinhashSignature getRandomSignature(){
		MinhashSignature s = new MinhashSignature(D);
		for(int i=0;i<s.size();i++){
			s.add((int) (Math.random()*vocabSize));
		}
		return s;
	}
	
	@Test
	public void testPermute() throws IOException{
		PermutationByBit p = new PermutationByBit(D);

		MinhashSignature s = getRandomSignature();
		System.out.println(s);

		int loopcnt = 0;
		MinhashSignature permutedS = new MinhashSignature(D);
		while(loopcnt++<10){
			ArrayListOfIntsWritable a = p.nextPermutation();
			s.perm(a,permutedS);
			for(int i=0;i<s.size();i++){
				assertTrue(permutedS.containsTerm(s.get(i)));
			}
			assertTrue(permutedS.size()==s.size());
			System.out.println(permutedS);
		}
	}
	
	@Test
	public void testReadWrite() throws IOException{
		MinhashSignature s = getRandomSignature();
		MinhashSignature s2 = getRandomSignature();

		FileSystem fs;
		SequenceFile.Writer w;
		Configuration conf = new Configuration();

		try {
			fs = FileSystem.get(conf);
			w = SequenceFile.createWriter(fs, conf, new Path("test"),
					IntWritable.class, MinhashSignature.class);	
			w.append(new IntWritable(1), s);
			w.append(new IntWritable(2), s2);
			w.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		List<PairOfWritables<WritableComparable, Writable>> listOfKeysPairs = SequenceFileUtils.readFile(new Path("test"));
		//	FileSystem.get(conf).delete(new Path("test"), true);

		MinhashSignature read1 = (MinhashSignature) listOfKeysPairs.get(0).getRightElement();
		MinhashSignature read2 = (MinhashSignature) listOfKeysPairs.get(1).getRightElement();
		
		assertTrue(read1.toString().equals(s.toString()));
		assertTrue(read2.toString().equals(s2.toString()));
		
		System.out.println(read1.toString());
		System.out.println(read2.toString());

	}

	@Test
	public void testSignatureSizeOnDisk() throws IOException{
		FileSystem fs;
		SequenceFile.Writer w;
		Configuration conf = new Configuration();

		try {
			MinhashSignature s = getRandomSignature();
			fs = FileSystem.get(conf);
			w = SequenceFile.createWriter(fs, conf, new Path("test2"),
					IntWritable.class, MinhashSignature.class);	
			for(int i=0;i<1000000;i++){
				w.append(new IntWritable(1), s);
			}
			w.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

//	@Test
//	public void testWrite() throws IOException{
//		MinhashSignature s = new MinhashSignature(1000);
//
//		FileSystem fs;
//		SequenceFile.Writer w;
//		Configuration conf = new Configuration();
//
//		try {
//			fs = FileSystem.get(conf);
//			w = SequenceFile.createWriter(fs, conf, new Path("test"),
//					IntWritable.class, MinhashSignature.class);	
//			int i=0;
//			while(i++<100){
//				if(i%100000==0)
//					System.out.println(i+"="+s);
//				w.append(new IntWritable(i), s);
//			}
//			w.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//	}

	@Test
	public void testBasic() {
		MinhashSignature s = new MinhashSignature(D);

		s.add(1);
		s.add(2);
		assertTrue(s.get(0)==1);
		assertTrue(s.get(1)==2);

		s.set(0, 3);
		assertTrue(s.get(0)==3);
	}


	@Test
	public void testHammingDistance(){
		MinhashSignature s1 = new MinhashSignature(D);
		MinhashSignature s2 = new MinhashSignature(D);
		s1.add(1);
		s1.add(2);
		s1.add(3);
		s1.add(4);
		s1.add(5);
		
		s2.add(3);
		s2.add(2);
		s2.add(5);
		s2.add(4);
		s2.add(1);

		assertTrue(s1.hammingDistance(s2)==3);
		assertTrue(s1.hammingDistance(s2,2)==3);
		assertTrue(s1.hammingDistance(s2,5)==3);
		
		for(int i=0;i<1000;i++){
			s1.hammingDistance(s2);
		}
		System.out.println(s1.hammingDistance(s2));

	}

	@Test
	public void testCompare(){
		MinhashSignature s1 = new MinhashSignature(D);
		MinhashSignature s2 = new MinhashSignature(D);
	
		s1.add(1);
		s1.add(2);
		s1.add(3);
		s1.add(4);
		s1.add(5);
		
		s2.add(3);
		s2.add(2);
		s2.add(5);
		s2.add(4);
		s2.add(1);

		assertTrue(s1.compareTo(s2)+"",s1.compareTo(s2)<0);
		assertTrue(s2.compareTo(s1)+"",s2.compareTo(s1)>0);
		assertTrue(s1.compareTo(s1)+"",s1.compareTo(s1)==0);
		assertTrue(s2.compareTo(s2)+"",s2.compareTo(s2)==0);

	}
	//	@Test
	//	public void testGetSlide(){
	//		BitsSignature s = new BitsSignature(10);
	//		BitsSignature s2 = new BitsSignature(10);
	//
	//		Slide slide = s.getSlide(0, 5);
	//		Slide slide2 = s.getSlide(0, 5);
	//
	//		System.out.println(slide);
	//		System.out.println(slide.hashCode());
	//		System.out.println(slide2);
	//		System.out.println(slide2.hashCode());
	//
	//		
	//		System.out.println(-299566668 % 100);
	//		System.out.println(Integer.MIN_VALUE);
	//		
	//		
	//		int i=0, j=0;
	//		float sum=0;
	//		while(i<100000000){
	//			while(j<100000000){
	//				float f1 = (float) Math.random();
	//				float f2 = (float) Math.random();
	//				float f = (f1*f2);
	//				sum = sum + f;
	//				j++;
	//			}
	//			i++;
	//		}
	//
	//		
	//	}
	
	@Test
	public void testSubSignature(){
		PermutationByBit p = new PermutationByBit(D);
		ArrayListOfIntsWritable a = p.nextPermutation();
		
		for(int i=0;i<100;i++){

			MinhashSignature s = getRandomSignature();

			System.out.println(s);

			MinhashSignature slide = s.getSubSignature(0, D/2);
			MinhashSignature slide2 = s.getSubSignature(D/2+1,D-1);
			System.out.println(slide+","+slide2);
			
			assertTrue(s.toString().equals(slide.toString()+","+slide2.toString()));
		}
		System.out.println("done");

		//				System.out.println(slide);
		//				System.out.println(slide2);

	}
	//	@Test
	//	public void testExtract(){
	//		BitsSignature s = new BitsSignature(10);
	//		s.set(2, true);
	//		//		s.set(4, true);
	//
	//		BitsSignature sub = s.getSubSignature(0, 4);
	//		System.out.println(sub);
	//		System.out.println(s);
	//
	//	}
}