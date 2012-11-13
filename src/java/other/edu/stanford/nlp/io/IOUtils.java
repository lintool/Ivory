package edu.stanford.nlp.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;

import edu.stanford.nlp.util.AbstractIterator;
import edu.stanford.nlp.util.ErasureUtils;
import edu.stanford.nlp.util.Generics;
import edu.stanford.nlp.util.StreamGobbler;
import edu.stanford.nlp.util.StringUtils;

/**
 * Helper Class for storing serialized objects to disk.
 *
 * @author Kayur Patel, Teg Grenager
 */

public class IOUtils {

  private static final int SLURPBUFFSIZE = 16000;

  public static final String eolChar = System.getProperty("line.separator");
  private static final String defaultEnc = "utf-8";

  // A class of static methods
  private IOUtils() {
  }

  /**
   * Write object to a file with the specified name.
   *
   * @param o
   *          object to be written to file
   * @param filename
   *          name of the temp file
   * @throws IOException
   *           If can't write file.
   * @return File containing the object
   */
  public static File writeObjectToFile(Object o, String filename)
  throws IOException {
    return writeObjectToFile(o, new File(filename));
  }

  /**
   * Write an object to a specified File.
   *
   * @param o
   *          object to be written to file
   * @param file
   *          The temp File
   * @throws IOException
   *           If File cannot be written
   * @return File containing the object
   */
  public static File writeObjectToFile(Object o, File file) throws IOException {
    return writeObjectToFile(o, file, false);
  }

  /**
   * Write an object to a specified File.
   *
   * @param o
   *          object to be written to file
   * @param file
   *          The temp File
   * @param append If true, append to this file instead of overwriting it
   * @throws IOException
   *           If File cannot be written
   * @return File containing the object
   */
  public static File writeObjectToFile(Object o, File file, boolean append) throws IOException {
    // file.createNewFile(); // cdm may 2005: does nothing needed
    ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(
        new GZIPOutputStream(new FileOutputStream(file, append))));
    oos.writeObject(o);
    oos.close();
    return file;
  }

  /**
   * Write object to a file with the specified name.
   *
   * @param o
   *          object to be written to file
   * @param filename
   *          name of the temp file
   *
   * @return File containing the object, or null if an exception was caught
   */
  public static File writeObjectToFileNoExceptions(Object o, String filename) {
    File file = null;
    ObjectOutputStream oos = null;
    try {
      file = new File(filename);
      // file.createNewFile(); // cdm may 2005: does nothing needed
      oos = new ObjectOutputStream(new BufferedOutputStream(
          new GZIPOutputStream(new FileOutputStream(file))));
      oos.writeObject(o);
      oos.close();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      closeIgnoringExceptions(oos);
    }
    return file;
  }

  /**
   * Write object to temp file which is destroyed when the program exits.
   *
   * @param o
   *          object to be written to file
   * @param filename
   *          name of the temp file
   * @throws IOException
   *           If file cannot be written
   * @return File containing the object
   */
  public static File writeObjectToTempFile(Object o, String filename)
  throws IOException {
    File file = File.createTempFile(filename, ".tmp");
    file.deleteOnExit();
    ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(
        new GZIPOutputStream(new FileOutputStream(file))));
    oos.writeObject(o);
    oos.close();
    return file;
  }

  /**
   * Write object to a temp file and ignore exceptions.
   *
   * @param o
   *          object to be written to file
   * @param filename
   *          name of the temp file
   * @return File containing the object
   */
  public static File writeObjectToTempFileNoExceptions(Object o, String filename) {
    try {
      return writeObjectToTempFile(o, filename);
    } catch (Exception e) {
      System.err.println("Error writing object to file " + filename);
      e.printStackTrace();
      return null;
    }
  }


  /**
   * Read an object from a stored file.
   *
   * @param file
   *          the file pointing to the object to be retrived
   * @throws IOException
   *           If file cannot be read
   * @throws ClassNotFoundException
   *           If reading serialized object fails
   * @return the object read from the file.
   */
  public static <T> T readObjectFromFile(File file) throws IOException,
  ClassNotFoundException {
    ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(
        new GZIPInputStream(new FileInputStream(file))));
    Object o = ois.readObject();
    ois.close();
    return ErasureUtils.<T> uncheckedCast(o);
  }

  public static <T> T readObjectFromObjectStream(ObjectInputStream ois) throws IOException,
  ClassNotFoundException {
    Object o = ois.readObject();
    return ErasureUtils.<T> uncheckedCast(o);
  }

  /**
   * Read an object from a stored file.
   *
   * @param filename
   *          The filename of the object to be retrieved
   * @throws IOException
   *           If file cannot be read
   * @throws ClassNotFoundException
   *           If reading serialized object fails
   * @return The object read from the file.
   */
  public static <T> T readObjectFromFile(String filename) throws IOException,
  ClassNotFoundException {
    return ErasureUtils
    .<T> uncheckedCast(readObjectFromFile(new File(filename)));
  }

  /**
   * Read an object from a stored file without throwing exceptions.
   *
   * @param file
   *          the file pointing to the object to be retrieved
   * @return the object read from the file, or null if an exception occurred.
   */
  public static <T> T readObjectFromFileNoExceptions(File file) {
    Object o = null;
    try {
      ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(
          new GZIPInputStream(new FileInputStream(file))));
      o = ois.readObject();
      ois.close();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return ErasureUtils.<T> uncheckedCast(o);
  }

  public static int lineCount(File textFile) throws IOException {
    BufferedReader r = new BufferedReader(new FileReader(textFile));
    int numLines = 0;
    while (r.readLine() != null) {
      numLines++;
    }
    return numLines;
  }

  public static ObjectOutputStream writeStreamFromString(String serializePath)
  throws IOException {
    ObjectOutputStream oos;
    if (serializePath.endsWith(".gz")) {
      oos = new ObjectOutputStream(new BufferedOutputStream(
          new GZIPOutputStream(new FileOutputStream(serializePath))));
    } else {
      oos = new ObjectOutputStream(new BufferedOutputStream(
          new FileOutputStream(serializePath)));
    }

    return oos;
  }

  public static ObjectInputStream readStreamFromString(String filenameOrUrl)
  throws IOException {
    ObjectInputStream in;
    InputStream is = getInputStreamFromURLOrClasspathOrFileSystem(filenameOrUrl);
    in = new ObjectInputStream(is);
    return in;
  }

  /**
   * Locates this file either in the CLASSPATH or in the file system. The CLASSPATH takes priority
   * @param fn
   * @return
   * @throws FileNotFoundException
   *         if the file does not exist
   */
  private static InputStream findStreamInClasspathOrFileSystem(String fn) throws FileNotFoundException {
    // ms 10-04-2010:
    // - even though this may look like a regular file, it may be a path inside a jar in the CLASSPATH
    // - check for this first. This takes precedence over the file system.
    InputStream is = IOUtils.class.getClassLoader().getResourceAsStream(fn);
    // if not found in the CLASSPATH, load from the file system
    if (is == null) is = new FileInputStream(fn);
    return is;
  }

  /**
   * Locates this file either using the given URL, or in the CLASSPATH, or in the file system
   * The CLASSPATH takes priority over the file system!
   * This stream is buffered and gzipped (if necessary)
   * @param textFileOrUrl
   * @return An InputStream for loading a resource
   * @throws IOException
   */
  public static InputStream 
    getInputStreamFromURLOrClasspathOrFileSystem(String textFileOrUrl)
    throws IOException 
  {
    InputStream in;
    if (textFileOrUrl.matches("https?://.*")) {
      URL u = new URL(textFileOrUrl);
      URLConnection uc = u.openConnection();
      in = uc.getInputStream();
    } else {
      try {
        in = findStreamInClasspathOrFileSystem(textFileOrUrl);
      } catch (FileNotFoundException e) {
        try {
          // Maybe this happens to be some other format of URL?
          URL u = new URL(textFileOrUrl);
          URLConnection uc = u.openConnection();
          in = uc.getInputStream();        
        } catch (IOException e2) {
          // TODO: freaking Java 1.5 didn't have an IOException that
          // could take a throwable as a cause
          throw new IOException("Unable to resolve \"" + 
                                textFileOrUrl + "\" as either " +
                                "class path, filename or URL");
        }
      }
    }

    // buffer this stream
    in = new BufferedInputStream(in);

    // gzip it if necessary
    if(textFileOrUrl.endsWith(".gz"))
      in = new GZIPInputStream(in);

    return in;
  }

  public static BufferedReader readReaderFromString(String textFileOrUrl)
  throws IOException {
    return new BufferedReader(new InputStreamReader(
        getInputStreamFromURLOrClasspathOrFileSystem(textFileOrUrl)));
  }

  /**
   * Open a BufferedReader to a file or URL specified by a String name. If the
   * String starts with https?://, then it is interpreted as a URL, otherwise it
   * is interpreted as a local file. If the String ends in .gz, it is
   * interpreted as a gzipped file (and uncompressed), else it is interpreted as
   * a regular text file in the given encoding.
   *
   * @param textFileOrUrl
   *          What to read from
   * @param encoding
   *          CharSet encoding
   * @return The BufferedReader
   * @throws IOException
   *           If there is an I/O problem
   */
  public static BufferedReader readReaderFromString(String textFileOrUrl,
      String encoding) throws IOException {
    InputStream is = getInputStreamFromURLOrClasspathOrFileSystem(textFileOrUrl);
    return new BufferedReader(new InputStreamReader(is, encoding));
  }

  /**
   * Returns an Iterable of the lines in the file.
   *
   * The file reader will be closed when the iterator is exhausted. IO errors
   * will throw an (unchecked) RuntimeIOException
   *
   * @param path
   *          The file whose lines are to be read.
   * @return An Iterable containing the lines from the file.
   */
  public static Iterable<String> readLines(String path) {
    return readLines(new File(path));
  }

  /**
   * Returns an Iterable of the lines in the file.
   *
   * The file reader will be closed when the iterator is exhausted.
   *
   * @param file
   *          The file whose lines are to be read.
   * @return An Iterable containing the lines from the file.
   */
  public static Iterable<String> readLines(final File file) {
    return readLines(file, null);
  }

  /**
   * Returns an Iterable of the lines in the file, wrapping the generated
   * FileInputStream with an instance of the supplied class. IO errors will
   * throw an (unchecked) RuntimeIOException
   *
   * @param file
   *          The file whose lines are to be read.
   * @param fileInputStreamWrapper
   *          The class to wrap the InputStream with, e.g. GZIPInputStream. Note
   *          that the class must have a constructor that accepts an
   *          InputStream.
   * @return An Iterable containing the lines from the file.
   */
  public static Iterable<String> readLines(final File file,
      final Class<? extends InputStream> fileInputStreamWrapper) {

    return new Iterable<String>() {
      public Iterator<String> iterator() {
        return new Iterator<String>() {

          protected BufferedReader reader = this.getReader();
          protected String line = this.getLine();

          public boolean hasNext() {
            return this.line != null;
          }

          public String next() {
            String nextLine = this.line;
            if (nextLine == null) {
              throw new NoSuchElementException();
            }
            line = getLine();
            return nextLine;
          }

          protected String getLine() {
            try {
              String result = this.reader.readLine();
              if (result == null) {
                this.reader.close();
              }
              return result;
            } catch (IOException e) {
              throw new RuntimeIOException(e);
            }
          }

          protected BufferedReader getReader() {
            try {
              InputStream stream = new FileInputStream(file);
              if (fileInputStreamWrapper != null) {
                stream = fileInputStreamWrapper.getConstructor(
                    InputStream.class).newInstance(stream);
              }
              return new BufferedReader(new InputStreamReader(stream));
            } catch (Exception e) {
              throw new RuntimeIOException(e);
            }
          }

          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  /**
   * Quietly opens a File. If the file ends with a ".gz" extension,
   * automatically opens a GZIPInputStream to wrap the constructed
   * FileInputStream.
   */
  public static InputStream openFile(File file) throws RuntimeIOException {
    try {
      InputStream is = new BufferedInputStream(new FileInputStream(file));
      if (file.getName().endsWith(".gz")) {
        is = new GZIPInputStream(is);
      }
      return is;
    } catch (Exception e) {
      throw new RuntimeIOException(e);
    }
  }

  /**
   * Provides an implementation of closing a file for use in a finally block so
   * you can correctly close a file without even more exception handling stuff.
   * From a suggestion in a talk by Josh Bloch.
   *
   * @param c
   *          The IO resource to close (e.g., a Stream/Reader)
   */
  public static void closeIgnoringExceptions(Closeable c) {
    if (c != null) {
      try {
        c.close();
      } catch (IOException ioe) {
        // ignore
      }
    }
  }

  /**
   * Iterate over all the files in the directory, recursively.
   *
   * @param dir
   *          The root directory.
   * @return All files within the directory.
   */
  public static Iterable<File> iterFilesRecursive(final File dir) {
    return iterFilesRecursive(dir, (Pattern) null);
  }

  /**
   * Iterate over all the files in the directory, recursively.
   *
   * @param dir
   *          The root directory.
   * @param ext
   *          A string that must be at the end of all files (e.g. ".txt")
   * @return All files within the directory ending in the given extension.
   */
  public static Iterable<File> iterFilesRecursive(final File dir,
      final String ext) {
    return iterFilesRecursive(dir, Pattern.compile(Pattern.quote(ext) + "$"));
  }

  /**
   * Iterate over all the files in the directory, recursively.
   *
   * @param dir
   *          The root directory.
   * @param pattern
   *          A regular expression that the file path must match. This uses
   *          Matcher.find(), so use ^ and $ to specify endpoints.
   * @return All files within the directory.
   */
  public static Iterable<File> iterFilesRecursive(final File dir,
      final Pattern pattern) {
    return new Iterable<File>() {
      public Iterator<File> iterator() {
        return new AbstractIterator<File>() {
          private final Queue<File> files = new LinkedList<File>(Collections
              .singleton(dir));
          private File file = this.findNext();

          @Override
          public boolean hasNext() {
            return this.file != null;
          }

          @Override
          public File next() {
            File result = this.file;
            if (result == null) {
              throw new NoSuchElementException();
            }
            this.file = this.findNext();
            return result;
          }

          private File findNext() {
            File next = null;
            while (!this.files.isEmpty() && next == null) {
              next = this.files.remove();
              if (next.isDirectory()) {
                files.addAll(Arrays.asList(next.listFiles()));
                next = null;
              } else if (pattern != null) {
                if (!pattern.matcher(next.getPath()).find()) {
                  next = null;
                }
              }
            }
            return next;
          }
        };
      }
    };
  }

  /**
   * Returns all the text in the given File.
   */
  public static String slurpFile(File file) throws IOException {
    Reader r = new FileReader(file);
    return IOUtils.slurpReader(r);
  }

  /**
   * Returns all the text in the given File.
   *
   * @param file The file to read from
   * @param encoding The character encoding to assume.  This may be null, and
   *       the platform default character encoding is used.
   */
  public static String slurpFile(File file, String encoding) throws IOException {
    Reader r;
    // InputStreamReader doesn't allow encoding to be null;
    if (encoding == null) {
      r = new InputStreamReader(new FileInputStream(file));
    } else {
      r = new InputStreamReader(new FileInputStream(file), encoding);
    }
    return IOUtils.slurpReader(r);
  }

  /**
   * Returns all the text in the given File.
   */
  public static String slurpGZippedFile(String filename) throws IOException {
    Reader r = new InputStreamReader(new GZIPInputStream(new FileInputStream(
        filename)));
    return IOUtils.slurpReader(r);
  }

  /**
   * Returns all the text in the given File.
   */
  public static String slurpGZippedFile(File file) throws IOException {
    Reader r = new InputStreamReader(new GZIPInputStream(new FileInputStream(
        file)));
    return IOUtils.slurpReader(r);
  }

  public static String slurpGBFileNoExceptions(String filename) {
    return IOUtils.slurpFileNoExceptions(filename, "GB18030");
  }

  /**
   * Returns all the text in the given file with the given encoding.
   */
  public static String slurpFile(String filename, String encoding)
  throws IOException {
    Reader r = new InputStreamReader(new FileInputStream(filename), encoding);
    return IOUtils.slurpReader(r);
  }
  
  public static String slurpFile(FSDataInputStream stream, String encoding)
  throws IOException {
    Reader r = new InputStreamReader(stream, encoding);
    return IOUtils.slurpReader(r);
  }

  /**
   * Returns all the text in the given file with the given encoding. If the file
   * cannot be read (non-existent, etc.), then and only then the method returns
   * <code>null</code>.
   */
  public static String slurpFileNoExceptions(String filename, String encoding) {
    try {
      return slurpFile(filename, encoding);
    } catch (Exception e) {
      throw new RuntimeIOException("slurpFile IO problem", e);
    }
  }
  
  public static String slurpFileNoExceptions(FSDataInputStream stream, String encoding) {
	    try {
	      return slurpFile(stream, encoding);
	    } catch (Exception e) {
	      throw new RuntimeIOException("slurpFile IO problem", e);
	    }
	  }

  public static String slurpGBFile(String filename) throws IOException {
    return slurpFile(filename, "GB18030");
  }

  /**
   * Returns all the text in the given file
   *
   * @return The text in the file.
   */
  public static String slurpFile(String filename) throws IOException {
    return IOUtils.slurpReader(new FileReader(filename));
  }

  /**
   * Returns all the text at the given URL.
   */
  public static String slurpGBURL(URL u) throws IOException {
    return IOUtils.slurpURL(u, "GB18030");
  }

  /**
   * Returns all the text at the given URL.
   */
  public static String slurpGBURLNoExceptions(URL u) {
    try {
      return slurpGBURL(u);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Returns all the text at the given URL.
   */
  public static String slurpURLNoExceptions(URL u, String encoding) {
    try {
      return IOUtils.slurpURL(u, encoding);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Returns all the text at the given URL.
   */
  public static String slurpURL(URL u, String encoding) throws IOException {
    String lineSeparator = System.getProperty("line.separator");
    URLConnection uc = u.openConnection();
    uc.setReadTimeout(30000);
    InputStream is;
    try {
      is = uc.getInputStream();
    } catch (SocketTimeoutException e) {
      // e.printStackTrace();
      System.err.println("Time out. Return empty string");
      return "";
    }
    BufferedReader br = new BufferedReader(new InputStreamReader(is, encoding));
    String temp;
    StringBuilder buff = new StringBuilder(16000); // make biggish
    while ((temp = br.readLine()) != null) {
      buff.append(temp);
      buff.append(lineSeparator);
    }
    br.close();
    return buff.toString();
  }

  /**
   * Returns all the text at the given URL.
   */
  public static String slurpURL(URL u) throws IOException {
    String lineSeparator = System.getProperty("line.separator");
    URLConnection uc = u.openConnection();
    InputStream is = uc.getInputStream();
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    String temp;
    StringBuilder buff = new StringBuilder(16000); // make biggish
    while ((temp = br.readLine()) != null) {
      buff.append(temp);
      buff.append(lineSeparator);
    }
    br.close();
    return buff.toString();
  }

  /**
   * Returns all the text at the given URL.
   */
  public static String slurpURLNoExceptions(URL u) {
    try {
      return slurpURL(u);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Returns all the text at the given URL.
   */
  public static String slurpURL(String path) throws Exception {
    return slurpURL(new URL(path));
  }

  /**
   * Returns all the text at the given URL. If the file cannot be read
   * (non-existent, etc.), then and only then the method returns
   * <code>null</code>.
   */
  public static String slurpURLNoExceptions(String path) {
    try {
      return slurpURL(path);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Returns all the text in the given File.
   *
   * @return The text in the file. May be an empty string if the file is empty.
   *         If the file cannot be read (non-existent, etc.), then and only then
   *         the method returns <code>null</code>.
   */
  public static String slurpFileNoExceptions(File file) {
    try {
      return IOUtils.slurpReader(new FileReader(file));
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Returns all the text in the given File.
   *
   * @return The text in the file. May be an empty string if the file is empty.
   *         If the file cannot be read (non-existent, etc.), then and only then
   *         the method returns <code>null</code>.
   */
  public static String slurpFileNoExceptions(String filename) {
    try {
      return slurpFile(filename);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Returns all the text from the given Reader.
   *
   * @return The text in the file.
   */
  public static String slurpReader(Reader reader) {
    BufferedReader r = new BufferedReader(reader);
    StringBuilder buff = new StringBuilder();
    try {
      char[] chars = new char[SLURPBUFFSIZE];
      while (true) {
        int amountRead = r.read(chars, 0, SLURPBUFFSIZE);
        if (amountRead < 0) {
          break;
        }
        buff.append(chars, 0, amountRead);
      }
      r.close();
    } catch (Exception e) {
      throw new RuntimeIOException("slurpReader IO problem", e);
    }
    return buff.toString();
  }

  /**
   * Send all bytes from the input stream to the output stream.
   *
   * @param input
   *          The input bytes.
   * @param output
   *          Where the bytes should be written.
   */
  public static void writeStreamToStream(InputStream input, OutputStream output)
  throws IOException {
    byte[] buffer = new byte[4096];
    while (true) {
      int len = input.read(buffer);
      if (len == -1) {
        break;
      }
      output.write(buffer, 0, len);
    }
  }

  /**
   * Read in a CSV formatted file with a header row
   * @param path - path to CSV file
   * @param quoteChar - character for enclosing strings, defaults to "
   * @param escapeChar - character for escaping quotes appearing in quoted strings; defaults to " (i.e. "" is used for " inside quotes, consistent with Excel)
   * @return a list of maps representing the rows of the csv. The maps' keys are the header strings and their values are the row contents
   * @throws IOException
   */
  public static List<Map<String,String>> readCSVWithHeader(String path, char quoteChar, char escapeChar) throws IOException {
    String[] labels = null;
    List<Map<String,String>> rows = Generics.newArrayList();
    for (String line : IOUtils.readLines(path)) {
      System.out.println("Splitting "+line);
      if (labels == null) {
        labels = StringUtils.splitOnCharWithQuoting(line,',','"',escapeChar);
      } else {
        String[] cells = StringUtils.splitOnCharWithQuoting(line,',',quoteChar,escapeChar);
        assert(cells.length == labels.length);
        Map<String,String> cellMap = new HashMap<String,String>();
        for (int i=0; i<labels.length; i++) cellMap.put(labels[i],cells[i]);
        rows.add(cellMap);
      }
    }
    return rows;
  }
  public static List<Map<String,String>> readCSVWithHeader(String path) throws IOException {
    return readCSVWithHeader(path,'"','"');
  }

  /**
   * Get a input file stream (automatically gunzip/bunzip2 depending on file extension)
   * @param filename Name of file to open
   * @return Input stream that can be used to read from the file
   * @throws IOException if there are exceptions opening the file
   */
  public static InputStream getFileInputStream(String filename) throws IOException {
    InputStream in = new FileInputStream(filename);
    if (filename.endsWith(".gz")) {
      in = new GZIPInputStream(in);
    } else if (filename.endsWith(".bz2")) {
      //in = new CBZip2InputStream(in);
      in = getBZip2PipedInputStream(filename);
    }
    return in;
  }

  /**
   * Get a output file stream (automatically gzip/bzip2 depending on file extension)
   * @param filename Name of file to open
   * @return Output stream that can be used to write to the file
   * @throws IOException if there are exceptions opening the file
   */
  public static OutputStream getFileOutputStream(String filename) throws IOException {
    OutputStream out = new FileOutputStream(filename);
    if (filename.endsWith(".gz")) {
      out = new GZIPOutputStream(out);
    } else if (filename.endsWith(".bz2")) {
      //out = new CBZip2OutputStream(out);
      out = getBZip2PipedOutputStream(filename);
    }
    return out;
  }

  public static BufferedReader getBufferedFileReader(String filename) throws IOException {
    return getBufferedFileReader(filename, defaultEnc);
  }

  public static BufferedReader getBufferedFileReader(String filename, String encoding) throws IOException {
    InputStream in = getFileInputStream(filename);
    return new BufferedReader(new InputStreamReader(in, encoding));
  }

  public static PrintWriter getPrintWriter(File textFile) throws IOException {
    File f = textFile.getAbsoluteFile();
    return new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f))));
  }

  public static PrintWriter getPrintWriter(String filename) throws IOException {
    return getPrintWriter(filename, defaultEnc);
  }

  public static PrintWriter getPrintWriter(String filename, String encoding) throws IOException {
    OutputStream out = getFileOutputStream(filename);
    return new PrintWriter(new BufferedWriter(new OutputStreamWriter(out, encoding)));
  }

  public static InputStream getBZip2PipedInputStream(String filename) throws IOException
  {
    String bzcat = System.getProperty("bzcat", "bzcat");
    Runtime rt = Runtime.getRuntime();
    String cmd = bzcat + " " + filename;
    //System.err.println("getBZip2PipedInputStream: Running command: "+cmd);
    Process p = rt.exec(cmd);
    Writer errWriter = new BufferedWriter(new OutputStreamWriter(System.err));
    StreamGobbler errGobler = new StreamGobbler(p.getErrorStream(), errWriter);
    errGobler.start();
    return p.getInputStream();
  }

  public static OutputStream getBZip2PipedOutputStream(String filename) throws IOException
  {
    return new BZip2PipedOutputStream(filename);
  }

  private static final Pattern tab = Pattern.compile("\t");
  /**
   * Read column as set
   * @param infile - filename
   * @param field  index of field to read
   * @return a set of the entries in column field
   * @throws IOException
   */
  public static Set<String> readColumnSet(String infile, int field) throws IOException
  {
    BufferedReader br = IOUtils.getBufferedFileReader(infile);
    String line;
    Set<String> set = new HashSet<String>();
    while ((line = br.readLine()) != null) {
      line = line.trim();
      if (line.length() > 0) {
        if (field < 0) {
          set.add(line);
        } else {
          String[] fields = tab.split(line);
          if (field < fields.length) {
            set.add(fields[field]);
          }
        }
      }
    }
    br.close();
    return set;
  }

  public static <C> List<C> readObjectFromColumns(Class objClass, String filename, String[] fieldNames, String delimiter)
  throws IOException, InstantiationException, IllegalAccessException,
  NoSuchFieldException, NoSuchMethodException, InvocationTargetException
  {
    Pattern delimiterPattern = Pattern.compile(delimiter);
    List<C> list = new ArrayList<C>();
    BufferedReader br = IOUtils.getBufferedFileReader(filename);
    String line;
    while ((line = br.readLine()) != null) {
      line = line.trim();
      if (line.length() > 0) {
        C item = StringUtils.<C>columnStringToObject(objClass, line, delimiterPattern, fieldNames);
        list.add(item);
      }
    }
    br.close();
    return list;
  }

  public static Map<String,String> readMap(String filename) throws IOException
  {
    Map<String,String> map = new HashMap<String,String>();
    try {
      BufferedReader br = IOUtils.getBufferedFileReader(filename);
      String line;
      while ((line = br.readLine()) != null) {
        String[] fields = tab.split(line,2);
        map.put(fields[0], fields[1]);
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    return map;
  }


  /**
   * Returns the contents of a file as a single string.  The string may be
   * empty, if the file is empty.  If there is an IOException, it is caught
   * and null is returned.
   */
  public static String stringFromFile(String filename) {
    return stringFromFile(filename,defaultEnc);
  }

  /**
   * Returns the contents of a file as a single string.  The string may be
   * empty, if the file is empty.  If there is an IOException, it is caught
   * and null is returned.  Encoding can also be specified.
   */
  public static String stringFromFile(String filename, String encoding) {
    try {
      StringBuilder sb = new StringBuilder();
      BufferedReader in = new BufferedReader(new EncodingFileReader(filename,encoding));
      String line;
      while ((line = in.readLine()) != null) {
        sb.append(line);
        sb.append(eolChar);
      }
      in.close();
      return sb.toString();
    }
    catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }


  /**
   * Returns the contents of a file as a list of strings.  The list may be
   * empty, if the file is empty.  If there is an IOException, it is caught
   * and null is returned.
   */
  public static List<String> linesFromFile(String filename) {
    return linesFromFile(filename,defaultEnc);
  }

  /**
   * Returns the contents of a file as a list of strings.  The list may be
   * empty, if the file is empty.  If there is an IOException, it is caught
   * and null is returned. Encoding can also be specified
   */
  public static List<String> linesFromFile(String filename,String encoding) {
    try {
      List<String> lines = new ArrayList<String>();
      BufferedReader in = new BufferedReader(new EncodingFileReader(filename,encoding));
      String line;
      while ((line = in.readLine()) != null) {
        lines.add(line);
      }
      in.close();
      return lines;
    }
    catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static String backupName(String filename) {
    return backupFile(new File(filename)).toString();
  }

  public static File backupFile(File file) {
    int max = 1000;
    String filename = file.toString();
    File backup = new File(filename + "~");
    if (!backup.exists()) { return backup; }
    for (int i = 1; i <= max; i++) {
      backup = new File(filename + ".~" + i + ".~");
      if (!backup.exists()) { return backup; }
    }
    return null;
  }

  public static boolean renameToBackupName(File file) {
    return file.renameTo(backupFile(file));
  }


  /**
   * A JavaNLP specific convenience routine for obtaining the current
   * scratch directory for the machine you're currently running on.
   */
  public static File getJNLPLocalScratch()  {
    try {
      String machineName = InetAddress.getLocalHost().getHostName().split("\\.")[0];
      String username = System.getProperty("user.name");
      return new File("/"+machineName+"/scr1/"+username);
    } catch (Exception e) {
      return new File("./scr/"); // default scratch
    }
  }

  /**
   * Given a filepath, makes sure a directory exists there.  If not, creates and returns it.
   * Same as ENSURE-DIRECTORY in CL.
   * @throws Exception
   */
  public static File ensureDir(File tgtDir) throws Exception {
    if (tgtDir.exists()) {
      if (tgtDir.isDirectory()) return tgtDir;
      else
        throw new Exception("Could not create directory "+tgtDir.getAbsolutePath()+", as a file already exists at that path.");
    } else {
      tgtDir.mkdirs();
      return tgtDir;
    }
  }

  public static void main(String[] args) {
    System.out.println(backupName(args[0]));
  }

  public static String getExtension(String fileName) {
    if(!fileName.contains("."))
      return null;
    int idx = fileName.lastIndexOf(".");
    return fileName.substring(idx+1);
  }

}
