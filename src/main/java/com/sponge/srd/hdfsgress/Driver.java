package com.sponge.srd.hdfsgress;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * Created by liuhb on 2014/12/8.
 */
public class Driver {
    public static void main(final String[] args) throws Exception, Configurator.MissingRequiredConfigException {
        int res = 0;
        String path= "E:\\快盘\\develop\\IdeaProjects\\hdfs-gress\\src\\main\\config\\gress.conf";
        Map<String, String> props = Configurator.loadProperties(path);
        for(Map.Entry<String, String > entry : props.entrySet()){
            System.out.println("Key: " + entry.getKey() + "\t" + "Value: " + entry.getValue());
        }

        System.out.println(Configurator.getConfigValue(props, Configurator.ConfigNames.MERGESCRIPT));
        System.out.println(Configurator.isOptionEnabled(props, Configurator.ConfigNames.CSVHEADER));
        System.out.println(Configurator.getConfigValue(props, Configurator.ConfigNames.UNCOMPRESSTYPE));

        System.exit(res);
    }
}

