package com.sdc.scala_example.command_line;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdc.scala_example.geometry.GeometryUtils;
import com.vividsolutions.jts.geom.Coordinate;


/**
 * This class is used to read and parse the command line arguments.
 * 
 * @author Simone De Cristofaro
 *         Nov 7, 2017
 */
public class CommandLineManager {

    private final static Logger LOG = LoggerFactory.getLogger(CommandLineManager.class);
    private static final String COMMAND_NAME = "spark-trial";

    public static final DateTimeFormatter CMD_LINE_DATE_TIME_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;
    
    private String[] arguments;
    private Options options;
    private Option optionHelp;
    private CommandLine cli;
    /**
     * <code>true</code> if the last call of the method {@link CommandLineManager#read(String[])}
     * read correctly the command line parameters returning <code>true</code>.
     */
    private boolean isArgRead;
    /**
     * <code>true</code> if the last call to {@link CommandLineManager#read(String[])} assert that
     * the specified arguments contains the help parameter.
     */
    private boolean hasHelp;


    private CommandLineManager() {
        super();
        initInternal();
        initOptions();
    }


    @SuppressWarnings("static-access")
    private void initOptions() {

        options = new Options();

        optionHelp = OptionBuilder.withLongOpt("help").hasArg(false).withDescription("shows this message").create("h");
        options.addOption(optionHelp);

        PARAMETER[] parameters = PARAMETER.values();

        for (PARAMETER parameter : parameters) {
            options.addOption(parameter.createOption());
        }

    }
    
    /**
     * 
     */
    private void initInternal() {

        isArgRead = false;
        hasHelp = false;
        cli = null;
    }
    
    /**
     * Read the command line parameters filling the internal {@link CommandLine} instance.
     * If the specified arguments contains the help parameter, it prints the help and don't fill the 
     * {@link CommandLine} instance. 
     * @param args
     * @return <code>true</code> if the internal {@link CommandLine} instance has been correctly filled.
     * @throws CommandLineManagerException
     */
    public boolean read(String[] args) throws CommandLineManagerException {

        initInternal();
        
        LOG.info("Reading command line parameters...");
        
        arguments = args;

        hasHelp = checkForHelp();
        
        if(!hasHelp) {
            
            CommandLineParser commandLineParser = new BasicParser();

            try {
                cli = commandLineParser.parse(options, arguments);
            }
            catch (ParseException e) {
                throw new CommandLineManagerException("Error reading command line arguments", e);
            }

            isArgRead = true;   
        }
        return isArgRead;
    }

    /**
     * check if the command line has help parameter. If so print it in the System.out.
     * 
     * @return <code>true</code> if the command line has help parameter
     * @throws CommandLineManagerException
     * @see HelpFormatter#printHelp(String, Options)
     */
    private boolean checkForHelp() throws CommandLineManagerException {

        CommandLineParser commandLineParser = new BasicParser();
        CommandLine commandLine = null;
        Options options = new Options();
        options.addOption(optionHelp);
        try {
            commandLine = commandLineParser.parse(options, arguments, true);
        }
        catch (ParseException e) {
            throw new CommandLineManagerException(String.format("Error checking for %s option", optionHelp.getLongOpt()), e);
        }
        boolean hasHelp = commandLine.hasOption(optionHelp.getOpt()); 
        if (hasHelp)
            printHelp();
        
        return hasHelp;

    }

    /**
     * 
     * @param mdfSparkSession
     * @return {@link MdfSparkJobContext}
     * @throws CommandLineManagerException
     */
    public AppContext parse() throws CommandLineManagerException {

        LOG.info("Parsing command line parameters...");

        if (!isArgRead)
            throw new CommandLineManagerException("Is not possible to parse the command line parameters because "
                    + "the arguments have not been read yet. Call \"read\" method before!");

        AppContext appContext = new AppContext();

        String runTypeValue = cli.getOptionValue(PARAMETER.RUN_TYPE.getLongOpt());
        RUN_TYPE runType = ParameterParser.parse(PARAMETER.RUN_TYPE, runTypeValue, input -> RUN_TYPE.parse(input));
        appContext.setRunType(runType);
        
        appContext.setSparkMaster(cli.getOptionValue(PARAMETER.SPARK_MASTER.getLongOpt()));

        appContext.setOsmNodesFilePath(cli.getOptionValue(PARAMETER.OSM_NODES_FILE.getLongOpt()));
        appContext.setOsmWaysFilePath(cli.getOptionValue(PARAMETER.OSM_WAYS_FILE.getLongOpt()));
        appContext.setNodesFilePath(cli.getOptionValue(PARAMETER.NODES_FILE.getLongOpt()));
        appContext.setLinksFilePath(cli.getOptionValue(PARAMETER.LINKS_FILE.getLongOpt()));
        appContext.setOutputDir(cli.getOptionValue(PARAMETER.OUTOUT_DIR.getLongOpt()));
        appContext.setCostFunction(cli.getOptionValue(PARAMETER.SP_COST_FUNCTION.getLongOpt(), ParameterDefault.DEFAULT_SP_COST_FUNCTION));

        
        Integer nodesRepartitionOutput = ParameterDefault.DEFAULT_NODES_REPARTITION_OUTPUT;
        if (cli.hasOption(PARAMETER.NODES_REPARTITION_OUTPUT.getLongOpt())) {
            String nodesRepartitionOutputValue = cli.getOptionValue(PARAMETER.NODES_REPARTITION_OUTPUT.getLongOpt());
            nodesRepartitionOutput = ParameterParser.parse(PARAMETER.NODES_REPARTITION_OUTPUT, nodesRepartitionOutputValue, (input) -> Integer.parseInt(input));
        }
        appContext.setNodesRepartitionOutput(nodesRepartitionOutput);

        Integer linksRepartitionOutput = ParameterDefault.DEFAULT_LINKS_REPARTITION_OUTPUT;
        if (cli.hasOption(PARAMETER.LINKS_REPARTITION_OUTPUT.getLongOpt())) {
            String linksRepartitionOutputValue = cli.getOptionValue(PARAMETER.LINKS_REPARTITION_OUTPUT.getLongOpt());
            linksRepartitionOutput = ParameterParser.parse(PARAMETER.LINKS_REPARTITION_OUTPUT, linksRepartitionOutputValue, (input) -> Integer.parseInt(input));
        }
        appContext.setLinksRepartitionOutput(linksRepartitionOutput);

        Double spSourceLon = null;
        if (cli.hasOption(PARAMETER.SP_SOURCE_LON.getLongOpt())) {
            String spSourceLonValue = cli.getOptionValue(PARAMETER.SP_SOURCE_LON.getLongOpt());
            spSourceLon = ParameterParser.parse(PARAMETER.SP_SOURCE_LON, spSourceLonValue, (input) -> Double.parseDouble(input));
        }
        
        Double spSourceLat = null;
        if (cli.hasOption(PARAMETER.SP_SOURCE_LAT.getLongOpt())) {
            String spSourceLatValue = cli.getOptionValue(PARAMETER.SP_SOURCE_LAT.getLongOpt());
            spSourceLat = ParameterParser.parse(PARAMETER.SP_SOURCE_LAT, spSourceLatValue, (input) -> Double.parseDouble(input));
        }
        
        appContext.setSpSource(GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(spSourceLon, spSourceLat)));
        
        return appContext;
    }

    public String getCurrentConfigParameterMessage() {

        StringBuilder sb = new StringBuilder("Current parameters value:").append(System.lineSeparator());

        PARAMETER[] parameters = PARAMETER.values();

        for (PARAMETER parameter : parameters) {
            String valueString = parameter.hasArg() ? cli.getOptionValue(parameter.getLongOpt()) : StringUtils.EMPTY;
            sb.append(String.format("%s: %s", parameter.getLongOpt(), valueString)).append(System.lineSeparator());
        }

        return sb.toString();

    }

    /**
     * 
     * @param optionLongOpt
     * @return String
     * @see CommandLine#getOptionValue(String)
     */
    public String getOptionValue(String optionLongOpt) {

        return cli.getOptionValue(optionLongOpt);
    }

    /**
     * @param commandLine 
     * @param parameter 
     * @param dtf
     * @param defaultInstant
     * @return {@link LocalDateTime}
     * @throws CommandLineManagerException 
     */
    protected LocalDateTime parseCompressedDateTime(CommandLine commandLine, PARAMETER parameter, DateTimeFormatter dtf, LocalDateTime defaultInstant) throws CommandLineManagerException {

        LocalDateTime instant = defaultInstant;

        if (commandLine.hasOption(parameter.getLongOpt())) {
            String instantValue = commandLine.getOptionValue(parameter.getLongOpt());
            instant = ParameterParser.parse(parameter, instantValue, (input) -> LocalDateTime.parse(instantValue, dtf));
            
        }
        return instant;
    }



    private void printHelp() {

        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(createUsageMessage().toString(), options);
    }


    private StringBuilder createUsageMessage() {

        StringBuilder usage = new StringBuilder("Test spark app for MDF.");
        usage.append(System.lineSeparator());
        usage.append(COMMAND_NAME).append(" [OPTIONS]");
        return usage;
    }

    
    /**
     * @return the {@link CommandLineManager#isArgRead}
     */
    public boolean isArgRead() {
    
        return isArgRead;
    }

    /**
     * @return the {@link CommandLineManager#hasHelp}
     */
    public boolean hasHelp() {
    
        return hasHelp;
    }
    
    public static Builder newBuilder() {

        return new Builder();
    }

    public static class Builder {

        private CommandLineManager clim;
        private String[] args;

        /**
         * 
         */
        private Builder() {
            super();
            clim = new CommandLineManager();
            args = new String[0];
        }

        public Builder withArgs(String[] args) {

            this.args = args;
            return this;
        }

        public CommandLineManager build() throws CommandLineManagerException {

            clim.read(args);
            return clim;
        }
    }

}
