package ivory.lsh;

import static org.junit.Assert.assertTrue;
import ivory.lsh.data.NBitSignature;
import ivory.lsh.data.PermutationByBit;
import ivory.lsh.data.SixtyFourBitSignature;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.pair.PairOfWritables;
import edu.umd.cloud9.io.SequenceFileUtils;


public class Signature64Test {
	@Test
	public void testPermuteNew() throws IOException{
		PermutationByBit p = new PermutationByBit(64);
		ArrayListOfIntsWritable a = p.nextPermutation();
		int[] permBlock = a.getArray();
		System.out.println(a);

		//create some signature of size 64 (=sizeof(long))
		SixtyFourBitSignature s = new SixtyFourBitSignature();
		for(int i=0;i<s.size();i++){
			s.set(i, (Math.random()>0.5 ? true : false));
		}
//		s.set(60, true);
		System.out.println(s);

		int loopcnt = 0;
		ByteArrayInputStream bis = new ByteArrayInputStream(s.getBits());
		DataInputStream in = new DataInputStream(bis);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		
		SixtyFourBitSignature permutedS = new SixtyFourBitSignature(64);
		while(loopcnt++<1000000){
			s.perm(a,permutedS);
//			System.out.println(permutedS);
		}
	}
//	@Test
//	public void testPermuteOld() throws IOException{
//		PermutationByBlock p = new PermutationByBlock(64);
//		ArrayListOfIntsWritable a = p.nextPermutation();
//		int[] permBlock = a.getArray();
//		System.out.println(a);
//
//
//		//create some signature of size 64 (=sizeof(long))
//		BitsSignature64 s = new BitsSignature64();
//		for(int i=0;i<s.size();i++){
//			s.set(i, (Math.random()>0.5 ? true : false));
//		}
//		System.out.println(s);
//
//		int loopcnt = 0;
//		while(loopcnt++<1){
//			//			System.out.println("loop "+loopcnt);
//			BitsSignature64 sPermOld1 = new BitsSignature64();
//			for(int j=0;j<s.size();j++){
//				//				System.out.println("bit "+j);
//				sPermOld1.set(j,s.get(permBlock[j]));
//			}
//			System.out.println(sPermOld1);
//
//		}
//	}
	@Test
	public void testReadWrite() throws IOException{
		SixtyFourBitSignature s = new SixtyFourBitSignature();
		SixtyFourBitSignature s2 = new SixtyFourBitSignature();

		for(int i=1;i<10;i=i*2){
			s.set(i, true);
			s2.set(i, true);
		}

		FileSystem fs;
		SequenceFile.Writer w;
		Configuration conf = new Configuration();

		try {
			fs = FileSystem.get(conf);
			w = SequenceFile.createWriter(fs, conf, new Path("test"),
					IntWritable.class, SixtyFourBitSignature.class);	
			w.append(new IntWritable(1), s);
			w.append(new IntWritable(2), s2);
			w.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		List<PairOfWritables<WritableComparable, Writable>> listOfKeysPairs = SequenceFileUtils.readFile(new Path("test"));
		//	FileSystem.get(conf).delete(new Path("test"), true);

		SixtyFourBitSignature read1 = (SixtyFourBitSignature) listOfKeysPairs.get(0).getRightElement();
		SixtyFourBitSignature read2 = (SixtyFourBitSignature) listOfKeysPairs.get(0).getRightElement();

		assertTrue(read1.get(1)==true);
		assertTrue(read1.get(5)==false);
		assertTrue(read1.toString().equals("0110100010000000000000000000000000000000000000000000000000000000"));

		System.out.println(read1.toString());
		System.out.println(read2.toString());

	}

	@Test
	public void testSignatureSizeOnDisk() throws IOException{
		FileSystem fs;
		SequenceFile.Writer w;
		Configuration conf = new Configuration();

		try {
			fs = FileSystem.get(conf);
			FSDataOutputStream out = fs.create(new Path("/Users/ferhanture/Documents/workspace/ivory/test3"));
			w = SequenceFile.createWriter(fs, conf, new Path("test2"),
					IntWritable.class, SixtyFourBitSignature.class);	
			for(int i=0;i<1000000;i++){
				SixtyFourBitSignature s =new SixtyFourBitSignature();
				w.append(new IntWritable(1), s);
				out.write(s.getBits());
			}
			w.close();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testWrite() throws IOException{
		SixtyFourBitSignature s = new SixtyFourBitSignature();

		FileSystem fs;
		SequenceFile.Writer w;
		Configuration conf = new Configuration();

		try {
			fs = FileSystem.get(conf);
			w = SequenceFile.createWriter(fs, conf, new Path("test"),
					IntWritable.class, SixtyFourBitSignature.class);	
			int i=0;
			while(i++<100){
				if(i%100000==0)
					System.out.println(i+"="+s);
				w.append(new IntWritable(i), s);
			}
			w.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Test
	public void testBasic() {
		SixtyFourBitSignature s = new SixtyFourBitSignature();
		for(int i=0;i<1000000000;i++)
			s.set(1, true);
//		s.set(1, false);
//		assertTrue(s.get(0));
//		assertTrue(!s.get(1));

//		s.set(0, false);
//		assertTrue(!s.get(0));
	}


	@Test
	public void testHammingDistance(){
		SixtyFourBitSignature s1 = new SixtyFourBitSignature();
		for(int i=1;i<63;i++){
			s1.set(i, true);
		}
		SixtyFourBitSignature s2 = new SixtyFourBitSignature();
		s2.set(0, true);
		s2.set(63, true);

		//		byte[] b1 = {107,125,3,6};
		//		byte[] b2 = {-26,125,6,3};

		//		int i1 = BitsSignature64.makeIntFromByte4(b1), i2 = BitsSignature64.makeIntFromByte4(b2);
		//		int n = (i1 ^ i2);
		//		
		//		int count = 32;
		//		n = ~n;
		//		while (n!=0)  {
		//			count-- ;
		//			n &= (n - 1) ;
		//		}

		//		 long count = n - ((n >> 1) & 033333333333) - ((n >> 2) & 011111111111);
		//		 count = ((count + (count >> 3)) & 030707070707) % 63;

		//		n = (byte) (n ^ (n-1));
		//		
		//		int count =8;
		//		while (n!=0)  {
		//			count-- ;
		//			n &= (n - 1) ;
		//		}
		//		
		//		System.out.println(count);

				assertTrue(s1.hammingDistance(s2)==64);
		//
		//		Signature s3 = new BitsSignature64(10);
		//		Signature s4 = new BitsSignature64(10);
		//
		//		for(int i=1;i<10;i=i*3){
		//			s3.set(i, true);
		//		}
		//		for(int i=1;i<7;i=i*2){
		//			s4.set(i, true);
		//		}
		//
		//		assertTrue(s3.hammingDistance(s4)==4);
	}

	@Test
	public void testCompare(){
		SixtyFourBitSignature s1 = new SixtyFourBitSignature();
		for(int i=0;i<63;i++){
			s1.set(i, true);
		}
		SixtyFourBitSignature s3 = new SixtyFourBitSignature();
		for(int i=0;i<63;i++){
			s3.set(i, true);
		}
		SixtyFourBitSignature s2 = new SixtyFourBitSignature();
		s2.set(0, true);
		s2.set(63, true);
		for(int i=0;i<10000;i++){
//				System.out.println(s1);
//				System.out.println(s2);
//				System.out.println(s1.compareTo(s2));
//				System.out.println(s2.compareTo(s1));
			s1.compareTo(s2);
		}
		assertTrue("s1 should be greater than s2",s1.compareTo(s2)>0);
		assertTrue("s2 should be less than s1",s2.compareTo(s1)<0);
//		assertTrue("s1 should be equal to s1",s1.compareTo(s1)==0);
//		assertTrue("s2 should be equal to s2",s1.compareTo(s1)==0);
		assertTrue(s1.compareTo(s3)==0);
		assertTrue(s3.compareTo(s1)==0);

		
	}
	
	@Test
	public void testSort(){
		//00000000
		SixtyFourBitSignature s1 = new SixtyFourBitSignature();
		for(int i=0;i<64;i++){
			s1.set(i, false);
		}
		
		//10000000
		SixtyFourBitSignature s2 = new SixtyFourBitSignature();
		s2.set(0, true);

		//01000000
		SixtyFourBitSignature s3 = new SixtyFourBitSignature();
		s3.set(1,true);
		
		//000100000
		SixtyFourBitSignature s4 = new SixtyFourBitSignature();
		s4.set(3,true);
	
		PriorityQueue<SixtyFourBitSignature> sorted = new PriorityQueue<SixtyFourBitSignature>();
		sorted.add(s1);
		sorted.add(s2);
		sorted.add(s3);
		sorted.add(s4);
		
	
		assertTrue(sorted.poll()==s1);
		assertTrue(sorted.poll()==s4);
		assertTrue(sorted.poll()==s3);
		assertTrue(sorted.poll()==s2);
		
		

	}
	//	@Test
	//	public void testGetSlide(){
	//		BitsSignature64 s = new BitsSignature64(10);
	//		BitsSignature64 s2 = new BitsSignature64(10);
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
		SixtyFourBitSignature s = new SixtyFourBitSignature();
		for(int i=1;i<10;i=i*3){
			s.set(i, true);
		}
		//		System.out.println(s);

//		for(int i=0;i<1;i++){
			NBitSignature slide = s.getSubSignature(0, 4);
			NBitSignature slide2 = s.getSubSignature(5,9);
//		}
			System.out.println(s);

				System.out.println("From 0 to 4: "+slide);
				System.out.println("From 5 to 9: "+slide2);

	}
	//	@Test
	//	public void testExtract(){
	//		BitsSignature64 s = new BitsSignature64(10);
	//		s.set(2, true);
	//		//		s.set(4, true);
	//
	//		BitsSignature64 sub = s.getSubSignature(0, 4);
	//		System.out.println(sub);
	//		System.out.println(s);
	//
	//	}
}
