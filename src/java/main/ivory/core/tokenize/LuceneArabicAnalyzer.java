package ivory.core.tokenize;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

public class LuceneArabicAnalyzer extends Tokenizer {
  ArabicAnalyzer a;
  TokenStream ts;
  
  @Override
  public void configure(Configuration conf) {
    a = new ArabicAnalyzer(Version.LUCENE_40);
  }

  @Override
  public void configure(Configuration mJobConf, FileSystem fs) {
    a = new ArabicAnalyzer(Version.LUCENE_40);
  }

  @Override
  public String[] processContent(String text) {
    List<String> tokens = new ArrayList<String>();
    try {
      ts = a.tokenStream("dummy", new StringReader(text));
      CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
      ts.clearAttributes();
      while (ts.incrementToken()) {
        tokens.add(termAtt.toString());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    String[] arr = new String[tokens.size()];
    return tokens.toArray(arr);
  }
  
  public static void main(String[] args) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException{
    if(args.length < 3){
      System.err.println("usage: [input] [language] [output-file]");
      System.exit(-1);
    }
//    ivory.core.tokenize.Tokenizer tokenizer = TokenizerFactory.createTokenizer(args[1], args[2], null);
    ivory.core.tokenize.Tokenizer tokenizer = new LuceneArabicAnalyzer();
    tokenizer.configure(null);
    BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args[2]), "UTF8"));
    BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(args[0]), "UTF8"));

    //    DataInput in = new DataInputStream(new BufferedInputStream(FileSystem.getLocal(new Configuration()).open(new Path(args[0]))));
    String line = null;
    while((line = in.readLine()) != null){
      String[] tokens = tokenizer.processContent(line);
      System.out.println("Found "+tokens.length+" tokens:");
      String s = "";
      for (String token : tokens) {
        s += token+"||";
      }
      out.write(s+"\n");
    }
    out.close();
  }

}
