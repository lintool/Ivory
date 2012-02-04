package ivory.core.tokenize;

import java.io.ByteArrayInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.html.BoilerpipeContentHandler;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;

public class TikaGalagoTokenizer extends Tokenizer {
  private Parser parser = new HtmlParser();
  private ParseContext parseContext = new ParseContext();
  private BoilerpipeContentHandler handler = new BoilerpipeContentHandler(new BodyContentHandler());
  private GalagoTokenizer galagoTokenizer = new GalagoTokenizer();

  @Override
  public String[] processContent(String text) {
    handler.recycle();
    try {
      parser.parse(new ByteArrayInputStream(text.getBytes()), handler, new Metadata(), parseContext);
    } catch (Exception e) {
      return galagoTokenizer.processContent(text);
    }

    String out = handler.toTextDocument().getContent().replaceAll("[\\p{Space}\u00A0]+", " ").trim();
    return galagoTokenizer.processContent(out.length() == 0 ? text : out);
  }

  @Override
  public void configure(Configuration conf, FileSystem fs) {}

  @Override
  public void configure(Configuration conf) {}
}
