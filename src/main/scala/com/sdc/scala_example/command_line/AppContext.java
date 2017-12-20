/**
 * AppContext.javaDistance
 */
package com.sdc.scala_example.command_line;

import com.vividsolutions.jts.geom.Point;

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
     * @param sparkMaster
     *            the {@link AppContext#sparkMaster} to set
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
    private Boolean osmConverterPersistNodes;
    private Boolean osmConverterPersistLinks;
    
    /**
     * @return the {@link AppContext#osmConverterPersistNodes}
     */
    public Boolean getOsmConverterPersistNodes() {
    
        return osmConverterPersistNodes;
    }

    
    /**
     * @param osmConverterPersistNodes the {@link AppContext#osmConverterPersistNodes} to set
     */
    public void setOsmConverterPersistNodes(Boolean osmConverterPersistNodes) {
    
        this.osmConverterPersistNodes = osmConverterPersistNodes;
    }

    
    /**
     * @return the {@link AppContext#osmConverterPersistLinks}
     */
    public Boolean getOsmConverterPersistLinks() {
    
        return osmConverterPersistLinks;
    }

    
    /**
     * @param osmConverterPersistLinks the {@link AppContext#osmConverterPersistLinks} to set
     */
    public void setOsmConverterPersistLinks(Boolean osmConverterPersistLinks) {
    
        this.osmConverterPersistLinks = osmConverterPersistLinks;
    }

    private String outputDir;
    private String costFunction;
    private Point spSource;
    private Integer spNearestDistance;
    private Integer spNearestAttempts;
    private Integer spNearestFactor;
    


    
    
    /**
     * @return the {@link AppContext#spNearestDistance}
     */
    public Integer getSpNearestDistance() {
    
        return spNearestDistance;
    }

    
    /**
     * @param spNearestDistance the {@link AppContext#spNearestDistance} to set
     */
    public void setSpNearestDistance(Integer spNearestDistance) {
    
        this.spNearestDistance = spNearestDistance;
    }

    
    /**
     * @return the {@link AppContext#spNearestAttempts}
     */
    public Integer getSpNearestAttempts() {
    
        return spNearestAttempts;
    }

    
    /**
     * @param spNearestAttempts the {@link AppContext#spNearestAttempts} to set
     */
    public void setSpNearestAttempts(Integer spNearestAttempts) {
    
        this.spNearestAttempts = spNearestAttempts;
    }

    
    /**
     * @return the {@link AppContext#spNearestFactor}
     */
    public Integer getSpNearestFactor() {
    
        return spNearestFactor;
    }

    
    /**
     * @param spNearestFactor the {@link AppContext#spNearestFactor} to set
     */
    public void setSpNearestFactor(Integer spNearestFactor) {
    
        this.spNearestFactor = spNearestFactor;
    }

    /**
     * @return the {@link AppContext#spSource}
     */
    public Point getSpSource() {
    
        return spSource;
    }

    
    /**
     * @param spSource the {@link AppContext#spSource} to set
     */
    public void setSpSource(Point spSource) {
    
        this.spSource = spSource;
    }

    /**
     * @return the {@link AppContext#costFunction}
     */
    public String getCostFunction() {

        return costFunction;
    }

    /**
     * @param costFunction
     *            the {@link AppContext#costFunction} to set
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
