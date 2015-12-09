package com.oozierunner.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by hpatel on 9/2/2015.
 */
public class HiveJdbcManager {

    private static OozieProperties oozieProperties = null;

    public HiveJdbcManager(String ooziePropertiesFile) {

        this.oozieProperties = OozieProperties.getInstance(ooziePropertiesFile);
    }

    public static Connection getHiveJdbcConnection() {
        try {

            //jdbc:hive2://host:port/database
            String hiveJdbcURL = "jdbc:hive2://" + oozieProperties.getProperty("HIVE_HOST") + ":" + oozieProperties.getProperty("HIVE_PORT") + "/" + oozieProperties.getProperty("HIVE_DATABASE");

            Class.forName("org.apache.hive.jdbc.HiveDriver");

            return DriverManager.getConnection(hiveJdbcURL, oozieProperties.getProperty("OOZIE_USER"), oozieProperties.getProperty("OOZIE_PASSWORD"));

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        return null;
    }
}
