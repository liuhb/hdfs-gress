package com.sponge.srd.hdfsgress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

/**
 * 封装程序使用的配置信息
 *
 *@author  刘红波
 *@version 0.1.0  2014/12/3.
 */
public class Config {
    private String datasource;
    private CompressionCodec codec;
    private boolean createLzopIndex;
    private Path srcDir;
    private Path workDir;
    private Path completeDir;
    private Path errorDir;
    private Path destDir;
    private Path destStagingDir;
    private String script;
    private String workScript;
    private boolean remove;
    private boolean verify;
    private int numThreads;
    private long pollSleepPeriodMillis;

    private boolean csvHeader;
    private String mergeScript;
    private String unCompressType;
    private boolean daemon;

    FileSystem srcFs;
    FileSystem destFs;
    Configuration config;

    public String getDatasource() {
        return datasource;
    }

    public Config setDatasource(String datasource) {
        this.datasource = datasource;
        return this;
    }

    public CompressionCodec getCodec() {
        return codec;
    }

    public Config setCodec(CompressionCodec codec) {
        this.codec = codec;
        return this;
    }

    public boolean isCreateLzopIndex() {
        return createLzopIndex;
    }

    public Config setCreateLzopIndex(boolean createLzopIndex) {
        this.createLzopIndex = createLzopIndex;
        return this;
    }

    public Path getSrcDir() {
        return srcDir;
    }

    public Config setSrcDir(Path srcDir) {
        this.srcDir = srcDir;
        return this;
    }

    public Path getWorkDir() {
        return workDir;
    }

    public Config setWorkDir(Path workDir) {
        this.workDir = workDir;
        return this;
    }

    public Path getCompleteDir() {
        return completeDir;
    }

    public Config setCompleteDir(Path completeDir) {
        this.completeDir = completeDir;
        return this;
    }

    public Path getErrorDir() {
        return errorDir;
    }

    public Config setErrorDir(Path errorDir) {
        this.errorDir = errorDir;
        return this;
    }

    public Path getDestDir() {
        return destDir;
    }

    public Config setDestDir(Path destDir) {
        this.destDir = destDir;
        return this;
    }

    public Path getDestStagingDir() {
        return destStagingDir;
    }

    public Config setDestStagingDir(Path destStagingDir) {
        this.destStagingDir = destStagingDir;
        return this;
    }

    public String getScript() {
        return script;
    }

    public Config setScript(String script) {
        this.script = script;
        return this;
    }

    public String getWorkScript() {
        return workScript;
    }

    public Config setWorkScript(String workScript) {
        this.workScript = workScript;
        return this;
    }

    public boolean isRemove() {
        return remove;
    }

    public Config setRemove(boolean remove) {
        this.remove = remove;
        return this;
    }

    public boolean isVerify() {
        return verify;
    }

    public Config setVerify(boolean verify) {
        this.verify = verify;
        return this;
    }

    public int getNumThreads() {
        return numThreads;
    }

    public Config setNumThreads(int numThreads) {
        this.numThreads = numThreads;
        return this;
    }

    public long getPollSleepPeriodMillis() {
        return pollSleepPeriodMillis;
    }

    public Config setPollSleepPeriodMillis(long pollSleepPeriodMillis) {
        this.pollSleepPeriodMillis = pollSleepPeriodMillis;
        return this;
    }

    public FileSystem getSrcFs() {
        return srcFs;
    }

    public Config setSrcFs(FileSystem srcFs) {
        this.srcFs = srcFs;
        return this;
    }

    public FileSystem getDestFs() {
        return destFs;
    }

    public Config setDestFs(FileSystem destFs) {
        this.destFs = destFs;
        return this;
    }

    public Configuration getConfig() {
        return config;
    }

    public Config setConfig(Configuration config) {
        this.config = config;
        return this;
    }

    public boolean isCsvHeader() {
        return csvHeader;
    }

    public Config setCsvHeader(boolean csvHeader) {
        this.csvHeader = csvHeader;
        return  this;
    }

    public String getMergeScript() {
        return mergeScript;
    }

    public Config setMergeScript(String mergeScript) {
        this.mergeScript = mergeScript;
        return this;
    }

    public String getUnCompressType() {
        return unCompressType;
    }

    public Config setUnCompressType(String unCompressType) {
        this.unCompressType = unCompressType;
        return this;
    }

    public boolean isDaemon() {
        return daemon;
    }

    public Config setDaemon(boolean daemon) {
        this.daemon = daemon;
        return this;
    }

    @Override
    public String toString() {
        return "Config{" +
                "datasource='" + datasource + '\'' +
                ", codec=" + codec +
                ", createLzopIndex=" + createLzopIndex +
                ", srcDir=" + srcDir +
                ", workDir=" + workDir +
                ", completeDir=" + completeDir +
                ", errorDir=" + errorDir +
                ", destDir=" + destDir +
                ", destStagingDir=" + destStagingDir +
                ", script='" + script + '\'' +
                ", workScript='" + workScript + '\'' +
                ", remove=" + remove +
                ", verify=" + verify +
                ", numThreads=" + numThreads +
                ", pollSleepPeriodMillis=" + pollSleepPeriodMillis +
                ", csvHeader=" + csvHeader +
                ", mergeScript='" + mergeScript + '\'' +
                ", unCompressType='" + unCompressType + '\'' +
                ", daemon=" + daemon +
                ", srcFs=" + srcFs +
                ", destFs=" + destFs +
                ", config=" + config +
                '}';
    }
}
