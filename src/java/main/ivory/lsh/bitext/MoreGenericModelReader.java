package ivory.lsh.bitext;

import java.io.IOException;
import java.io.InputStream;
import opennlp.maxent.io.GISModelReader;
import opennlp.model.AbstractModel;
import opennlp.model.AbstractModelReader;
import opennlp.model.DataReader;
import opennlp.model.PlainTextFileDataReader;
import opennlp.perceptron.PerceptronModelReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class MoreGenericModelReader {
  private static final Logger sLogger = Logger.getLogger(MoreGenericModelReader.class);

  private AbstractModelReader delegateModelReader;

  public MoreGenericModelReader(String f, FileSystem localFs) throws IOException {
    this(new Path(f), localFs);
  }
  
  public MoreGenericModelReader(Path f, FileSystem localFs) throws IOException {
    sLogger.setLevel(Level.DEBUG);
    
    sLogger.debug(f);
    InputStream modelIn = localFs.open(f);
    sLogger.debug("input stream created: "+modelIn);
    DataReader reader = new PlainTextFileDataReader(modelIn);
    String modelType = reader.readUTF();
    sLogger.debug("model type: "+modelType);
    if (modelType.equals("Perceptron")) {
      delegateModelReader = new PerceptronModelReader(reader);
    }else if (modelType.equals("GIS")) {
      delegateModelReader = new GISModelReader(reader);
    }else {
      throw new IOException("Unknown model format: "+modelType);
    }
  }

  public AbstractModel constructModel() throws IOException {
    return delegateModelReader.constructModel();
  }

}