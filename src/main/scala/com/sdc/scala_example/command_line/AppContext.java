/**
 * AppContext.java
 */
package com.sdc.scala_example.command_line;

/**
 * @author Simone De Cristofaro
 *         Dec 18, 2017
 */
public class AppContext {

    private String sparkMaster;
    
    /**
     * @return the {@link AppContext#sparkMaster}
     */
    public String getSparkMaster() {
    
        return sparkMaster;
    }

    
    /**
     * @param sparkMaster the {@link AppContext#sparkMaster} to set
     */
    public void setSparkMaster(String sparkMaster) {
    
        this.sparkMaster = sparkMaster;
    }

    private RUN_TYPE runType;
    private String osmNodesFilePath;
    private String osmWaysFilePath;
    private String nodesFilePath;
    private String linksFilePath;
    private Integer nodesRepartitionOutput;
    private Integer linksRepartitionOutput;
    private String outputDir;
    private String costFunction;

    
    /**
     * @return the {@link AppContext#costFunction}
     */
    public String getCostFunction() {
    
        return costFunction;
    }


    
    /**
     * @param costFunction the {@link AppContext#costFunction} to set
     */
    public void setCostFunction(String costFunction) {
    
        this.costFunction = costFunction;
    }


    /**
     * @return the {@link AppContext#runType}
     */
    public RUN_TYPE getRunType() {

        return runType;
    }

    /**
     * @param runType
     *            the {@link AppContext#runType} to set
     */
    public void setRunType(RUN_TYPE runType) {

        this.runType = runType;
    }

    /**
     * @return the {@link AppContext#osmNodesFilePath}
     */
    public String getOsmNodesFilePath() {

        return osmNodesFilePath;
    }

    /**
     * @param osmNodesFilePath
     *            the {@link AppContext#osmNodesFilePath} to set
     */
    public void setOsmNodesFilePath(String osmNodesFilePath) {

        this.osmNodesFilePath = osmNodesFilePath;
    }

    /**
     * @return the {@link AppContext#osmWaysFilePath}
     */
    public String getOsmWaysFilePath() {

        return osmWaysFilePath;
    }

    /**
     * @param osmWaysFilePath
     *            the {@link AppContext#osmWaysFilePath} to set
     */
    public void setOsmWaysFilePath(String osmWaysFilePath) {

        this.osmWaysFilePath = osmWaysFilePath;
    }

    /**
     * @return the {@link AppContext#nodesFilePath}
     */
    public String getNodesFilePath() {

        return nodesFilePath;
    }

    /**
     * @param nodesFilePath
     *            the {@link AppContext#nodesFilePath} to set
     */
    public void setNodesFilePath(String nodesFilePath) {

        this.nodesFilePath = nodesFilePath;
    }

    /**
     * @return the {@link AppContext#linksFilePath}
     */
    public String getLinksFilePath() {

        return linksFilePath;
    }

    /**
     * @param linksFilePath
     *            the {@link AppContext#linksFilePath} to set
     */
    public void setLinksFilePath(String linksFilePath) {

        this.linksFilePath = linksFilePath;
    }

    /**
     * @return the {@link AppContext#nodesRepartitionOutput}
     */
    public Integer getNodesRepartitionOutput() {

        return nodesRepartitionOutput;
    }

    /**
     * @param nodesRepartitionOutput
     *            the {@link AppContext#nodesRepartitionOutput} to set
     */
    public void setNodesRepartitionOutput(Integer nodesRepartitionOutput) {

        this.nodesRepartitionOutput = nodesRepartitionOutput;
    }

    /**
     * @return the {@link AppContext#linksRepartitionOutput}
     */
    public Integer getLinksRepartitionOutput() {

        return linksRepartitionOutput;
    }

    /**
     * @param linksRepartitionOutput
     *            the {@link AppContext#linksRepartitionOutput} to set
     */
    public void setLinksRepartitionOutput(Integer linksRepartitionOutput) {

        this.linksRepartitionOutput = linksRepartitionOutput;
    }

    /**
     * @return the {@link AppContext#outputDir}
     */
    public String getOutputDir() {

        return outputDir;
    }

    /**
     * @param outputDir
     *            the {@link AppContext#outputDir} to set
     */
    public void setOutputDir(String outputDir) {

        this.outputDir = outputDir;
    }

}
