package ivory.lsh;

import java.io.IOException;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;
import edu.umd.hooka.ttables.TTable_monolithic_IFAs;
import ivory.core.util.CLIRUtils;

public class CLIRUtilsTest extends TestCase {

	public void testGIZA(){
		String srcVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/giza/vocab.de-en.de";
		String trgVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/giza/vocab.de-en.en";
		String ttableFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/giza/ttable.de-en";
		
		Configuration conf =  new Configuration();
		try {
			CLIRUtils.createTTableFromGIZA(
					"/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/giza/lex.0-0.f2n", 
					srcVocabFile, 
					trgVocabFile, 
					ttableFile, 
					FileSystem.getLocal(conf));
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
		}
	}
	
	public void testGIZA2(){
		String srcVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/giza/vocab.en-de.en";
		String trgVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/giza/vocab.en-de.de";
		String ttableFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/giza/ttable.en-de";
		
		Configuration conf =  new Configuration();
		try {
			CLIRUtils.createTTableFromGIZA(
					"/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/giza/lex.0-0.n2f", 
					srcVocabFile, 
					trgVocabFile, 
					ttableFile, 
					FileSystem.getLocal(conf));
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
		}
	}

	public void testBerkeleyAligner(){
		String srcVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/berkeleyaligner/vocab.de-en.de";
		String trgVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/berkeleyaligner/vocab.de-en.en";
		String ttableFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/berkeleyaligner/ttable.de-en";

		Configuration conf =  new Configuration();
		try {
			CLIRUtils.createTTableFromBerkeleyAligner(
					"/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/berkeleyaligner/stage2.2.params.txt", 
					srcVocabFile, 
					trgVocabFile, 
					ttableFile, 
					FileSystem.getLocal(conf));
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
			assertTrue("Berk_case1 --> "+ttable.get(src, trg), ttable.get(src, trg) > 0.3);
			
			src = srcVocab.get("krankenhaus");
			trg = trgVocab.get("hospit");
			assertTrue("Berk_case2 --> "+ttable.get(src, trg), ttable.get(src, trg) > 0.3);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void testBerkeleyAligner2(){
		String srcVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/berkeleyaligner/vocab.en-de.en";
		String trgVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/berkeleyaligner/vocab.en-de.de";
		String ttableFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/berkeleyaligner/ttable.en-de";

		Configuration conf =  new Configuration();
		try {
			CLIRUtils.createTTableFromBerkeleyAligner(
					"/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/berkeleyaligner/stage2.1.params.txt", 
					srcVocabFile, 
					trgVocabFile, 
					ttableFile, 
					FileSystem.getLocal(conf));
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
		}
	}

	public void testHooka(){
		String srcVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/hooka/vocab.de-en.de.raw";
		String trgVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/hooka/vocab.de-en.en.raw";
		String ttableFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/hooka/ttable.de-en.raw";
		String finalSrcVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/hooka/vocab.de-en.de";
		String finalTrgVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/hooka/vocab.de-en.en";
		String finalTTableFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/hooka/ttable.de-en";
		
		Configuration conf =  new Configuration();
		try {
			CLIRUtils.createTTableFromHooka(
					srcVocabFile, 
					trgVocabFile, 
					ttableFile, 
					finalSrcVocabFile, 
					finalTrgVocabFile, 
					finalTTableFile, 
					FileSystem.getLocal(conf));
		} catch (IOException e) {
			e.printStackTrace();
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
		}
	}
	
	public void testHooka2(){
		String srcVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/hooka/vocab.en-de.en.raw";
		String trgVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/hooka/vocab.en-de.de.raw";
		String ttableFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/hooka/ttable.en-de.raw";
		String finalSrcVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/hooka/vocab.en-de.en";
		String finalTrgVocabFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/hooka/vocab.en-de.de";
		String finalTTableFile = "/Users/ferhanture/edu/research_archive/data/de-en/europarl-v6.de-en/hooka/ttable.en-de";
		
		Configuration conf =  new Configuration();
		try {
			CLIRUtils.createTTableFromHooka(
					srcVocabFile, 
					trgVocabFile, 
					ttableFile, 
					finalSrcVocabFile, 
					finalTrgVocabFile, 
					finalTTableFile, 
					FileSystem.getLocal(conf));
		} catch (IOException e) {
			e.printStackTrace();
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
		}
	}

}
