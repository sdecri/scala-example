package com.sdc.graphx_example.command_line;

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

import com.sdc.graphx_example.geometry.GeometryUtils;
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


    private void initOptions() {

        options = new Options();


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
        @SuppressWarnings("static-access")
        Option optionHelp = OptionBuilder.withLongOpt("help").hasArg(false).withDescription("shows this message").create("h");

        Options options = new Options();
        options.addOption(optionHelp);
        try {
            commandLine = commandLineParser.parse(options, arguments, true);
        }
        catch (ParseException e) {
            throw new CommandLineManagerException(String.format("Error checking for %s option", optionHelp.getLongOpt()), e);
        }
        boolean hasHelp = commandLine.hasOption(optionHelp.getOpt()) || commandLine.hasOption(optionHelp.getLongOpt()); 
        LOG.info(String.format("Has help: %s", hasHelp));
        if (hasHelp)
            printHelp();
        
        return hasHelp;

    }

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

        String networkOutputFormatValue = cli.getOptionValue(PARAMETER.OSM_CONVERTER_NETWORK_OUTPUT_FORMAT.getLongOpt());
        NETWORK_OUTPUT_FORMAT networkOutputFormat = ParameterParser.parse(PARAMETER.OSM_CONVERTER_NETWORK_OUTPUT_FORMAT, networkOutputFormatValue, input -> NETWORK_OUTPUT_FORMAT.parse(input));
        appContext.setNetworkOutputFormat(networkOutputFormat);
        
        
        appContext.setOsmNodesFilePath(cli.getOptionValue(PARAMETER.OSM_NODES_FILE.getLongOpt()));
        appContext.setOsmWaysFilePath(cli.getOptionValue(PARAMETER.OSM_WAYS_FILE.getLongOpt()));
        appContext.setNodesFilePath(cli.getOptionValue(PARAMETER.NODES_FILE.getLongOpt()));
        appContext.setLinksFilePath(cli.getOptionValue(PARAMETER.LINKS_FILE.getLongOpt()));
        appContext.setOutputDir(cli.getOptionValue(PARAMETER.OUTOUT_DIR.getLongOpt()));
        appContext.setCostFunction(cli.getOptionValue(PARAMETER.SP_COST_FUNCTION.getLongOpt(), ParameterDefault.DEFAULT_SP_COST_FUNCTION));

        
        Integer nodesRepartitionOutput = ParameterDefault.DEFAULT_OSM_CONVERTER_NODES_REPARTITION_OUTPUT;
        if (cli.hasOption(PARAMETER.OSM_CONVERTER_NODES_REPARTITION_OUTPUT.getLongOpt())) {
            String nodesRepartitionOutputValue = cli.getOptionValue(PARAMETER.OSM_CONVERTER_NODES_REPARTITION_OUTPUT.getLongOpt());
            nodesRepartitionOutput = ParameterParser.parse(PARAMETER.OSM_CONVERTER_NODES_REPARTITION_OUTPUT, nodesRepartitionOutputValue, (input) -> Integer.parseInt(input));
        }
        appContext.setNodesRepartitionOutput(nodesRepartitionOutput);

        Integer linksRepartitionOutput = ParameterDefault.DEFAULT_OSM_CONVERTER_LINKS_REPARTITION_OUTPUT;
        if (cli.hasOption(PARAMETER.OSM_CONVERTER_LINKS_REPARTITION_OUTPUT.getLongOpt())) {
            String linksRepartitionOutputValue = cli.getOptionValue(PARAMETER.OSM_CONVERTER_LINKS_REPARTITION_OUTPUT.getLongOpt());
            linksRepartitionOutput = ParameterParser.parse(PARAMETER.OSM_CONVERTER_LINKS_REPARTITION_OUTPUT, linksRepartitionOutputValue, (input) -> Integer.parseInt(input));
        }
        appContext.setLinksRepartitionOutput(linksRepartitionOutput);

        Boolean osmConverterPersistNodes = ParameterDefault.DEFAULT_OSM_CONVERTER_PERSIST_NODES;
        if (cli.hasOption(PARAMETER.OSM_CONVERTER_PERSIST_NODES.getLongOpt())) {
            String osmConverterPersistNodesValue = cli.getOptionValue(PARAMETER.OSM_CONVERTER_PERSIST_NODES.getLongOpt());
            osmConverterPersistNodes = ParameterParser.parse(PARAMETER.OSM_CONVERTER_PERSIST_NODES, osmConverterPersistNodesValue, (input) -> Boolean.parseBoolean(input));
        }
        appContext.setOsmConverterPersistNodes(osmConverterPersistNodes);
        
        Boolean osmConverterPersistLinks = ParameterDefault.DEFAULT_OSM_CONVERTER_PERSIST_LINKS;
        if (cli.hasOption(PARAMETER.OSM_CONVERTER_PERSIST_LINKS.getLongOpt())) {
            String osmConverterPersistLinksValue = cli.getOptionValue(PARAMETER.OSM_CONVERTER_PERSIST_LINKS.getLongOpt());
            osmConverterPersistLinks = ParameterParser.parse(PARAMETER.OSM_CONVERTER_PERSIST_LINKS, osmConverterPersistLinksValue, (input) -> Boolean.parseBoolean(input));
        }
        appContext.setOsmConverterPersistLinks(osmConverterPersistLinks);     
        
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
        
        if(spSourceLon != null && spSourceLat != null)
            appContext.setSpSource(GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(spSourceLon, spSourceLat)));
        
        Integer spNearestDistance = ParameterDefault.DEFAULT_SP_NEAREST_DISTANCE;
        if (cli.hasOption(PARAMETER.SP_NEAREST_DISTANCE.getLongOpt())) {
            String spNearestDistanceValue = cli.getOptionValue(PARAMETER.SP_NEAREST_DISTANCE.getLongOpt());
            spNearestDistance = ParameterParser.parse(PARAMETER.SP_NEAREST_DISTANCE, spNearestDistanceValue, (input) -> Integer.parseInt(input));
        }
        appContext.setSpNearestDistance(spNearestDistance);

        Integer spNearestAttempts = ParameterDefault.DEFAULT_SP_NEAREST_ATTEMPS;
        if (cli.hasOption(PARAMETER.SP_NEAREST_ATTEMPS.getLongOpt())) {
            String spNearestAttemptsValue = cli.getOptionValue(PARAMETER.SP_NEAREST_ATTEMPS.getLongOpt());
            spNearestAttempts = ParameterParser.parse(PARAMETER.SP_NEAREST_ATTEMPS, spNearestAttemptsValue, (input) -> Integer.parseInt(input));
        }
        appContext.setSpNearestAttempts(spNearestAttempts);
        
        Integer spNearestFactor = ParameterDefault.DEFAULT_SP_NEAREST_FACTOR;
        if (cli.hasOption(PARAMETER.SP_NEAREST_FACTOR.getLongOpt())) {
            String spNearestFactorValue = cli.getOptionValue(PARAMETER.SP_NEAREST_FACTOR.getLongOpt());
            spNearestFactor = ParameterParser.parse(PARAMETER.SP_NEAREST_FACTOR, spNearestFactorValue, (input) -> Integer.parseInt(input));
        }
        appContext.setSpNearestFactor(spNearestFactor);      
        
        Integer spRandomGraphNumVertices = ParameterDefault.DEFAULT_SP_RANDOM_GRAPH_NUM_VERTICES;
        if (cli.hasOption(PARAMETER.SP_RANDOM_GRAPH_NUM_VERTICES.getLongOpt())) {
            String spRandomGraphNumVerticesValue = cli.getOptionValue(PARAMETER.SP_RANDOM_GRAPH_NUM_VERTICES.getLongOpt());
            spRandomGraphNumVertices = ParameterParser.parse(PARAMETER.SP_RANDOM_GRAPH_NUM_VERTICES, spRandomGraphNumVerticesValue, (input) -> Integer.parseInt(input));
        }
        appContext.setSpRandomGraphNumVertices(spRandomGraphNumVertices);         
        
        Integer spGraphRepartition = ParameterDefault.DEFAULT_SP_GRAPH_REPARTITION;
        if (cli.hasOption(PARAMETER.SP_GRAPH_REPARTITION.getLongOpt())) {
            String spGraphRepartitionValue = cli.getOptionValue(PARAMETER.SP_GRAPH_REPARTITION.getLongOpt());
            spGraphRepartition = ParameterParser.parse(PARAMETER.SP_GRAPH_REPARTITION, spGraphRepartitionValue, (input) -> Integer.parseInt(input));
        }
        appContext.setSpGraphRepartition(spGraphRepartition);
        
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

        StringBuilder usage = new StringBuilder("Spark graphx trial");
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
