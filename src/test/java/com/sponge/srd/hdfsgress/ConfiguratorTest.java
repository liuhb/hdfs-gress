package com.sponge.srd.hdfsgress;

import org.junit.Before;
import org.junit.Test;

import java.io.FileWriter;
import java.util.Map;

import static org.junit.Assert.*;

public class ConfiguratorTest {

    private static  String  confPath = "target/testgress.txt";

    @Before
    public void setUp() throws Exception {
        FileWriter out = new FileWriter(confPath);
        out.write("DATASOURCE_NAME = test\n");
        out.write("SRC_DIR = file:/tmp/gress/in\n");
        out.write("WORK_DIR = file:/tmp/gress/work\n");
        out.write("COMPLETE_DIR = file:/tmp/gress/complete\n");
        out.write("ERROR_DIR = file:/tmp/gress/error\n");
        out.write("DEST_STAGING_DIR = hdfs:/incoming/stage\n");
        out.write("DEST_DIR = hdfs:/incoming\n");
        out.close();
    }

    @Test
    public void testLoadProperty() throws Exception {
        Map<String, String> props = Configurator.loadProperties(confPath);
        assertEquals(Configurator.getConfigValue(props, Configurator.ConfigNames.DEST_DIR),"hdfs:/incoming");
        assertEquals(Configurator.getConfigValue(props, Configurator.ConfigNames.MERGESCRIPT), null);
        assertEquals(Configurator.isOptionEnabled(props, Configurator.ConfigNames.DAEMON), false);
        assertEquals(Configurator.isOptionEnabled(props, Configurator.ConfigNames.CSVHEADER), false);
        assertEquals(Configurator.isOptionEnabled(props, Configurator.ConfigNames.VERIFY), false);
    }

}