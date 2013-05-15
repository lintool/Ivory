package ivory.bloomir.util;

import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Utility class that helps manage command line options
 *
 * @author Nima Asadi
 */
public class OptionManager {
  public static final String INDEX_ROOT_PATH = "index";
  public static final String POSTINGS_ROOT_PATH = "posting";
  public static final String BLOOM_ROOT_PATH = "bloom";
  public static final String QUERY_PATH = "query";
  public static final String OUTPUT_PATH = "output";
  public static final String SPAM_PATH = "spam";
  public static final String BITS_PER_ELEMENT = "bpe";
  public static final String NUMBER_OF_HASH = "nbHash";
  public static final String HITS = "hits";
  public static final String DOCUMENT_VECTOR_CLASS = "dvclass";
  public static final String DOCUMENT_PATH = "document";
  public static final String JUDGMENT_PATH = "judgment";
  public static final String FEATURE_PATH = "feature";

  private Options options;
  private CommandLine cmdline;
  private String className;
  private Map<String, List<String>> dependencies;

  /**
   * @param className Class name (arbitrary)
   */
  public OptionManager(String className) {
    this.className = Preconditions.checkNotNull(className);

    options = new Options();
    dependencies = Maps.newHashMap();
    cmdline = null;
  }

  /**
   * Adds a dependency. If option A is present
   * then option B will be required when parsing command
   * line arguments.
   *
   * Please use {@link #addOption} to define optionA and
   * optionB first.
   *
   * @param optionA
   * @param optionB Option B is required if option A is present
   */
  public void addDependency(String optionA, String optionB) {
    Preconditions.checkNotNull(optionA);
    Preconditions.checkNotNull(optionB);
    Preconditions.checkArgument(options.hasOption(optionA), "Option " + optionA + " not defined yet!");
    Preconditions.checkArgument(options.hasOption(optionB), "Option " + optionB + " not defined yet!");

    if(!dependencies.containsKey(optionA)) {
      List<String> required = Lists.newArrayList();
      dependencies.put(optionA, required);
    }
    dependencies.get(optionA).add(optionB);
  }

  /**
   * Creates an Option
   *
   * @param option Option name
   * @param valueType Value type
   * @param description Description of the option
   * @param isRequired Whether this option is required or optional
   */
  public void addOption(String option, String valueType, String description, boolean isRequired) {
    addOption(option, valueType, description, isRequired, true);
  }

  /**
   * Creates an Option
   *
   * @param option Option name
   * @param valueType Value type
   * @param description Description of the option
   * @param isRequired Whether this option is required or optional
   * @param hasArg Whether this option takes argument
   */
  @SuppressWarnings("static-access")
  public void addOption(String option, String valueType, String description, boolean isRequired,
                        boolean hasArg) {
    Preconditions.checkNotNull(option);
    Preconditions.checkNotNull(valueType);
    Preconditions.checkNotNull(description);

    options.addOption(OptionBuilder.withArgName(valueType).hasArg(hasArg).isRequired(isRequired)
                .withDescription(description).create(option));
  }

  /**
   * Parses command line arguments
   *
   * @param args Command line arguments
   */
  public void parse(String[] args) throws ParseException {
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
      checkDependencies();
    } catch(ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage() + "\n");
      printHelp();
      throw exp;
    }
  }

  /**
   * Please use {@link #parse} before calling this method.
   *
   * @param option Option name
   * @return value of the given option
   */
  public String getOptionValue(String option) {
    Preconditions.checkNotNull(cmdline, "Command line arguments not parsed yet!");
    Preconditions.checkArgument(options.hasOption(option), "Option " + option + " does not exist.");
    return cmdline.getOptionValue(option);
  }

  /**
   * Please use {@link #parse} before calling this method.
   *
   * @param option Option name
   * @return whether the given option is present in the command line arguments
   */
  public boolean foundOption(String option) {
    Preconditions.checkNotNull(cmdline, "Command line arguments not parsed yet!");
    Preconditions.checkArgument(options.hasOption(option), "Option " + option + " does not exist.");
    return cmdline.hasOption(option);
  }

  private void checkDependencies() throws ParseException {
    Preconditions.checkNotNull(cmdline, "Command line arguments not parsed yet!");

    for(Object option: options.getRequiredOptions()) {
      if(!cmdline.hasOption((String) option)) {
        throw new ParseException("Option \"" + option + "\" not found.");
      }
    }

    for(String optionA: dependencies.keySet()) {
      if(!cmdline.hasOption(optionA)) {
        continue;
      }
      for(String optionB: dependencies.get(optionA)) {
        if(!cmdline.hasOption(optionB)) {
          throw new ParseException("Option \"" + optionB + "\" not found.");
        }
      }
    }
  }

  private void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(className, options);

    if(dependencies.size() == 0) {
      return;
    }
    System.out.println("\nDependencies:");
    for(String optionA: dependencies.keySet()) {
      System.out.print("If option \"" + optionA + "\" is given, options {");
      for(String optionB: dependencies.get(optionA)) {
        System.out.print("\"" + optionB + "\", ");
      }
      System.out.println("} are required.");
    }
    System.out.println();
  }
}
