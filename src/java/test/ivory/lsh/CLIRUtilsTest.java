package ivory.lsh;

import java.io.IOException;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;
import ivory.core.util.CLIRUtils;

public class CLIRUtilsTest extends TestCase {
	  private static final String GIZA_DIR = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/giza";
	  private static final String HOOKA_DIR = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/hooka";
	  private static final String BERKELEY_DIR = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/berkeleyaligner";

	@Test
	public void testGIZA(){
		String srcVocabFile = GIZA_DIR+"/vocab.de-en.de";
		String trgVocabFile = GIZA_DIR+"/vocab.de-en.en";
		String ttableFile = GIZA_DIR+"/ttable.de-en";
		
		Configuration conf =  new Configuration();
		try {
			CLIRUtils.createTTableFromGIZA(
					GIZA_DIR+"/lex.0-0.f2n", 
					srcVocabFile, 
					trgVocabFile, 
					ttableFile, 
					0.9f, 15, FileSystem.getLocal(conf));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			Vocab srcVocab = HadoopAlign.loadVocab(new Path(srcVocabFile), FileSystem.getLocal(conf));
			Vocab trgVocab = HadoopAlign.loadVocab(new Path(trgVocabFile), FileSystem.getLocal(conf));
			TTable_monolithic_IFAs ttable = new TTable_monolithic_IFAs(FileSystem.getLocal(conf), new Path(ttableFile), true);
			
			assertTrue(ttable.getMaxE()==srcVocab.size()-1);

			int src = srcVocab.get("buch");
			int trg = trgVocab.get("book");
			assertTrue("Giza_case1 --> "+ttable.get(src, trg), ttable.get(src, trg) > 0.3);
			
			src = srcVocab.get("krankenhaus");
			trg = trgVocab.get("hospit");
			assertTrue("Giza_case2 --> "+ttable.get(src, trg), ttable.get(src, trg) > 0.3);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void testGIZA2(){
		String srcVocabFile = GIZA_DIR+"/vocab.en-de.en";
		String trgVocabFile = GIZA_DIR+"/vocab.en-de.de";
		String ttableFile = GIZA_DIR+"/ttable.en-de";
		
		Configuration conf =  new Configuration();
		try {
			CLIRUtils.createTTableFromGIZA(
					GIZA_DIR+"/lex.0-0.n2f", 
					srcVocabFile, 
					trgVocabFile, 
					ttableFile, 
					0.9f, 15, FileSystem.getLocal(conf));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			Vocab srcVocab = HadoopAlign.loadVocab(new Path(srcVocabFile), FileSystem.getLocal(conf));
			Vocab trgVocab = HadoopAlign.loadVocab(new Path(trgVocabFile), FileSystem.getLocal(conf));
			TTable_monolithic_IFAs ttable = new TTable_monolithic_IFAs(FileSystem.getLocal(conf), new Path(ttableFile), true);
		
			assertTrue(ttable.getMaxE()==srcVocab.size()-1);
			
			int src = srcVocab.get("book");
			int trg = trgVocab.get("buch");
			assertTrue("Giza_case1 --> "+ttable.get(src, trg), ttable.get(src, trg) > 0.3);
			
			src = srcVocab.get("hospit");
			trg = trgVocab.get("krankenhaus");
			assertTrue("Giza_case2 --> "+ttable.get(src, trg), ttable.get(src, trg) > 0.3);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testBerkeleyAligner(){
		String srcVocabFile = BERKELEY_DIR+"/vocab.de-en.de";
		String trgVocabFile = BERKELEY_DIR+"/vocab.de-en.en";
		String ttableFile = BERKELEY_DIR+"/ttable.de-en";

		Configuration conf =  new Configuration();
		try {
			CLIRUtils.createTTableFromBerkeleyAligner(
					BERKELEY_DIR+"/stage2.2.params.txt", 
					srcVocabFile, 
					trgVocabFile, 
					ttableFile, 
					0.9f, 15, FileSystem.getLocal(conf));
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		
		try {
			Vocab srcVocab = HadoopAlign.loadVocab(new Path(srcVocabFile), FileSystem.getLocal(conf));
			Vocab trgVocab = HadoopAlign.loadVocab(new Path(trgVocabFile), FileSystem.getLocal(conf));
			TTable_monolithic_IFAs ttable = new TTable_monolithic_IFAs(FileSystem.getLocal(conf), new Path(ttableFile), true);
			
			assertTrue(ttable.getMaxE()==srcVocab.size()-1);

			int src = srcVocab.get("buch");
			int trg = trgVocab.get("book");
			assertTrue("Berk_case1 --> "+ttable.get(src, trg), ttable.get(src, trg) > 0.3);
			
			src = srcVocab.get("krankenhaus");
			trg = trgVocab.get("hospit");
			assertTrue("Berk_case2 --> "+ttable.get(src, trg), ttable.get(src, trg) > 0.3);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void testBerkeleyAligner2(){
		String srcVocabFile = BERKELEY_DIR+"/vocab.en-de.en";
		String trgVocabFile = BERKELEY_DIR+"/vocab.en-de.de";
		String ttableFile = BERKELEY_DIR+"/ttable.en-de";

		Configuration conf =  new Configuration();
		try {
			CLIRUtils.createTTableFromBerkeleyAligner(
					BERKELEY_DIR+"/stage2.1.params.txt", 
					srcVocabFile, 
					trgVocabFile, 
					ttableFile, 
					0.9f, 15, FileSystem.getLocal(conf));
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			Vocab srcVocab = HadoopAlign.loadVocab(new Path(srcVocabFile), FileSystem.getLocal(conf));
			Vocab trgVocab = HadoopAlign.loadVocab(new Path(trgVocabFile), FileSystem.getLocal(conf));
			TTable_monolithic_IFAs ttable = new TTable_monolithic_IFAs(FileSystem.getLocal(conf), new Path(ttableFile), true);
			
			assertTrue(ttable.getMaxE()==srcVocab.size()-1);

			int src = srcVocab.get("book");
			int trg = trgVocab.get("buch");
			assertTrue("Berk_case1 --> "+ttable.get(src, trg), ttable.get(src, trg) > 0.3);
			
			src = srcVocab.get("hospit");
			trg = trgVocab.get("krankenhaus");
			assertTrue("Berk_case2 --> "+ttable.get(src, trg), ttable.get(src, trg) > 0.3);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	@Test
	public void testHooka(){
		String srcVocabFile = HOOKA_DIR+"/vocab.de-en.de.raw";
		String trgVocabFile = HOOKA_DIR+"/vocab.de-en.en.raw";
		String ttableFile = HOOKA_DIR+"/ttable.de-en.raw";
		String finalSrcVocabFile = HOOKA_DIR+"/vocab.de-en.de";
		String finalTrgVocabFile = HOOKA_DIR+"/vocab.de-en.en";
		String finalTTableFile = HOOKA_DIR+"/ttable.de-en";
		
		Configuration conf =  new Configuration();
		try {
			CLIRUtils.createTTableFromHooka(
					srcVocabFile, 
					trgVocabFile, 
					ttableFile, 
					finalSrcVocabFile, 
					finalTrgVocabFile, 
					finalTTableFile, 
					0.9f, 15, FileSystem.getLocal(conf));
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		
		try {
			Vocab srcVocab = HadoopAlign.loadVocab(new Path(finalSrcVocabFile), FileSystem.getLocal(conf));
			Vocab trgVocab = HadoopAlign.loadVocab(new Path(finalTrgVocabFile), FileSystem.getLocal(conf));
			TTable_monolithic_IFAs ttable = new TTable_monolithic_IFAs(FileSystem.getLocal(conf), new Path(finalTTableFile), true);
			
			assertTrue(ttable.getMaxE()==srcVocab.size()-1);

			int src = srcVocab.get("buch");
			int trg = trgVocab.get("book");
			assertTrue("Hooka_case1 --> "+ttable.get(src, trg), ttable.get(src, trg) > 0.3);
			
			src = srcVocab.get("krankenhaus");
			trg = trgVocab.get("hospit");
			assertTrue("Hooka_case2 --> "+ttable.get(src, trg), ttable.get(src, trg) > 0.3);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void testHooka2(){
		String srcVocabFile = HOOKA_DIR+"/vocab.en-de.en.raw";
		String trgVocabFile = HOOKA_DIR+"/vocab.en-de.de.raw";
		String ttableFile = HOOKA_DIR+"/ttable.en-de.raw";
		String finalSrcVocabFile = HOOKA_DIR+"/vocab.en-de.en";
		String finalTrgVocabFile = HOOKA_DIR+"/vocab.en-de.de";
		String finalTTableFile = HOOKA_DIR+"/ttable.en-de";
		
		Configuration conf =  new Configuration();
		try {
			CLIRUtils.createTTableFromHooka(
					srcVocabFile, 
					trgVocabFile, 
					ttableFile, 
					finalSrcVocabFile, 
					finalTrgVocabFile, 
					finalTTableFile, 
					0.9f, 15, FileSystem.getLocal(conf));
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
		
		try {
			Vocab srcVocab = HadoopAlign.loadVocab(new Path(finalSrcVocabFile), FileSystem.getLocal(conf));
			Vocab trgVocab = HadoopAlign.loadVocab(new Path(finalTrgVocabFile), FileSystem.getLocal(conf));
			TTable_monolithic_IFAs ttable = new TTable_monolithic_IFAs(FileSystem.getLocal(conf), new Path(finalTTableFile), true);
			
			assertTrue(ttable.getMaxE()==srcVocab.size()-1);

			int src = srcVocab.get("book");
			int trg = trgVocab.get("buch");
			assertTrue("Hooka_case1 --> "+ttable.get(src, trg), ttable.get(src, trg) > 0.3);
			
			src = srcVocab.get("hospit");
			trg = trgVocab.get("krankenhaus");
			assertTrue("Hooka_case2 --> "+ttable.get(src, trg), ttable.get(src, trg) > 0.3);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

}
