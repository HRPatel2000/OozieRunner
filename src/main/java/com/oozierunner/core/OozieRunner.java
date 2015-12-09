package com.oozierunner.core;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Created by hpatel on 8/31/2015.
 *
 * IMPORTANT : Each Oozie Functional Test should extend OozieRunner
 */
public class OozieRunner {

    private final static String domainName = System.getProperty("domainName");
    private static final String ooziePropertiesFile = System.getProperty("ooziePropertiesFile");
    private static final String jobPropertiesFile = System.getProperty("jobPropertiesFile");

    private static OozieProperties oozieProperties = null;
    private static OozieServiceManager oozieServiceManager = null;

    @BeforeClass
    public static void initialize() {

        System.out.println("Domain Name :-> " + domainName);
        oozieProperties = OozieProperties.getInstance(ooziePropertiesFile);

        String testDataSourceFile = oozieProperties.getProperty(domainName + "." + "test_data_source_file");

        String testDataTargetFile = oozieProperties.getProperty(domainName + "." + "test_data_target_file");

        oozieServiceManager = new OozieServiceManager(ooziePropertiesFile, jobPropertiesFile);
        oozieServiceManager.submitOozieJob(domainName, testDataSourceFile, testDataTargetFile);
    }

    public OozieServiceManager getOozieServiceManager() {

        return this.oozieServiceManager;
    }

    public OozieProperties getOozieProperties() {

        return this.oozieProperties;
    }

    @AfterClass
    public static void cleanup() {

    }
}
