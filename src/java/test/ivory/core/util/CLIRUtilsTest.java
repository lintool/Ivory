package ivory.core.util;

import java.io.File;
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

public class CLIRUtilsTest extends TestCase {
  @Test
  public void testGIZA(){
    String srcVocabFile = "etc/toy_vocab.de-en.de";
    String trgVocabFile = "etc/toy_vocab.de-en.en";
    String ttableFile = "etc/toy_ttable.de-en";

    Configuration conf =  new Configuration();
    try {
      CLIRUtils.createTTableFromGIZA(
          "etc/toy_lex.0-0.f2n", 
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
    String srcVocabFile = "etc/vocab.en-de.en";
    String trgVocabFile = "etc/vocab.en-de.de";
    String ttableFile = "etc/ttable.en-de";

    Configuration conf =  new Configuration();
    try {
      CLIRUtils.createTTableFromGIZA(
          "etc/toy_lex.0-0.n2f", 
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

    new File(srcVocabFile).delete();
    new File(trgVocabFile).delete();
    new File(ttableFile).delete();
  }

  @Test
  public void testBerkeleyAligner(){
    String srcVocabFile = "etc/vocab.de-en.de";
    String trgVocabFile = "etc/vocab.de-en.en";
    String ttableFile = "etc/ttable.de-en";

    Configuration conf =  new Configuration();
    try {
      CLIRUtils.createTTableFromBerkeleyAligner(
          "etc/toy_stage2.2.params.txt", 
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

    new File(srcVocabFile).delete();
    new File(trgVocabFile).delete();
    new File(ttableFile).delete();
  }

  @Test
  public void testBerkeleyAligner2(){
    String srcVocabFile = "etc/vocab.en-de.en";
    String trgVocabFile = "etc/vocab.en-de.de";
    String ttableFile = "etc/ttable.en-de";

    Configuration conf =  new Configuration();
    try {
      CLIRUtils.createTTableFromBerkeleyAligner(
          "etc/toy_stage2.1.params.txt", 
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

    new File(srcVocabFile).delete();
    new File(trgVocabFile).delete();
    new File(ttableFile).delete();
  }

  @Test
  public void testHooka(){
    String finalSrcVocabFile = "etc/toy_vocab.de-en.de";
    String finalTrgVocabFile = "etc/toy_vocab.de-en.en";
    String finalTTableFile = "etc/toy_ttable.de-en";

    Configuration conf =  new Configuration();
    try {
      CLIRUtils.createTTableFromHooka(
          "etc/toy_vocab.de-en.de.raw", 
          "etc/toy_vocab.de-en.en.raw", 
          "etc/toy_ttable.de-en.raw", 
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
    String finalSrcVocabFile = "etc/toy_vocab.en-de.en";
    String finalTrgVocabFile = "etc/toy_vocab.en-de.de";
    String finalTTableFile = "etc/toy_ttable.en-de";

    Configuration conf =  new Configuration();
    try {
      CLIRUtils.createTTableFromHooka(
          "etc/toy_vocab.en-de.en.raw", 
          "etc/toy_vocab.en-de.de.raw", 
          "etc/toy_ttable.en-de.raw", 
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
