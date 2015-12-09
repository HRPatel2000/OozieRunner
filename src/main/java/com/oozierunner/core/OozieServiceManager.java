package com.oozierunner.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

/**
 * Created by hpatel on 8/31/2015.
 */
public class OozieServiceManager {

    private static OozieProperties oozieProperties = null;
    private String jobPropertiesFile = null;

    private HiveJdbcManager hiveJdbcManager = null;

    public OozieServiceManager(String ooziePropertiesFile, String jobPropertiesFile) {

        this.oozieProperties = OozieProperties.getInstance(ooziePropertiesFile);
        this.jobPropertiesFile = jobPropertiesFile;
        this.hiveJdbcManager = new HiveJdbcManager(ooziePropertiesFile);
    }

    public void submitOozieJob(String domainName, String hdfsTestDataSourceFile, String hdfsTestDataTargetFile) {

        this.copyTestData(domainName, hdfsTestDataSourceFile, hdfsTestDataTargetFile);

        String OOZIE_URL = "http://" + this.oozieProperties.getProperty("OOZIE_HOST") + ":" + this.oozieProperties.getProperty("OOZIE_PORT") + "/oozie/";
        System.out.println(domainName + " :-> OOZIE_URL :-> " + OOZIE_URL);

        // get a OozieClient for local Oozie
        //http://host:port/oozie/;
        OozieClient wc = new OozieClient(OOZIE_URL);

        Properties conf = this.loadWorkflowProperties(this.jobPropertiesFile);
        conf.setProperty(OozieClient.USER_NAME, this.oozieProperties.getProperty("OOZIE_USER"));
        System.out.println(domainName + " :-> Workflow Properties :-> " + conf);

        // submit and start the workflow job
        String jobId = null;

        try {
            // wait until the workflow job finishes printing the status every 10 seconds
            jobId = wc.run(conf);
            System.out.println(domainName + " :-> Oozie Workflow JOB Submitted : JobID :-> " + jobId);

            while (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
                System.out.println(domainName + " :-> Oozie Workflow JOB[ " + jobId + " ] Still Running...");
                Thread.sleep(10 * 1000);
            }

            if(wc.getJobInfo(jobId).getStatus() != WorkflowJob.Status.SUCCEEDED) {

                throw new OozieClientException("ERR_CODE_0212", domainName + " : Oozie Job Failed or Suspended or Killed - EXITING...");
            }

            // print the final status o the workflow job
            System.out.println(domainName + " :-> Oozie Workflow JOB Completed...");
            System.out.println(wc.getJobInfo(jobId));

        } catch (OozieClientException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param domainName
     *
     * hadoop fs -cp /projects/ddsw/dev/data/backup/dealer_hierarchy/<<DOMAIN_NAME>>/<<FILE_NAME>> /projects/ddsw/dev/data/raw/nas/<<DOMAIN_NAME>>
     */
    public void copyTestData(String domainName, String hdfsTestDataSourceFile, String hdfsTestDataTargetFile) {

        System.out.println(domainName + " :-> Test Data Loading :: PENDING!!!");
        try {

            String hdfsURI = "hdfs://" + this.oozieProperties.getProperty("HDFS_HOST") + ":" + this.oozieProperties.getProperty("HDFS_PORT"); //hdfs://host:port

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hdfsURI);
            conf.set("user.name", this.oozieProperties.getProperty("OOZIE_USER"));
            conf.set("basedir", "/user/" + this.oozieProperties.getProperty("OOZIE_USER"));
            FileSystem hdfs = FileSystem.get(conf);
            System.out.println(domainName + " :-> HDFS :-> " + hdfs);

            System.out.println(domainName + " :-> HDFSHomeDirectory :-> " + hdfs.getHomeDirectory());
            System.out.println(domainName + " :-> HDFS-URI :-> " + hdfs.getUri());
            System.out.println(domainName + " :-> HDFSWorkingDirectory :-> " + hdfs.getWorkingDirectory());
            System.out.println(domainName + " :-> HDFS : " + hdfs + " : Exists :-> " + hdfs.exists(hdfs.getHomeDirectory()));

            Path hdfsTestDataSource = new Path(hdfsURI + hdfsTestDataSourceFile);
            Path hdfsTestDataTarget = new Path(hdfsURI + hdfsTestDataTargetFile);

            System.out.println(domainName + " :-> HDFS TEST DATA : " + hdfsTestDataSource + " : Exists :-> " + hdfs.exists(hdfsTestDataSource));
            System.out.println(domainName + " :-> HDFS DOMAIN DATA : " + hdfsTestDataTarget + " : Exists :-> " + hdfs.exists(hdfsTestDataTarget));

            FileUtil hdfsUtil = new FileUtil();

            hdfsUtil.copy(
                    hdfs,
                    hdfsTestDataSource,
                    hdfs,
                    hdfsTestDataTarget,
                    false,
                    true,
                    conf
            );

            System.out.println(domainName + " :-> NOW : HDFS TEST DATA : " + hdfsTestDataSource + " : Exists :-> " + hdfs.exists(hdfsTestDataSource));
            System.out.println(domainName + " :-> NOW : HDFS DOMAIN DATA : " + hdfsTestDataTarget + " : Exists :-> " + hdfs.exists(hdfsTestDataTarget));

            /**
             * IMPORTANT
             * If the Source Data file on HDFS is not owned by the Hive/Hadoop User, then use the command below to
             * change the permission for Hive/Hadoop User to move/delete the file once processed...
             */
            hdfs.setPermission(hdfsTestDataTarget, new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ_EXECUTE));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String executeHiveQuery(String query, String fileName, String delimeter, String domainName) {

        BufferedWriter bufferedWriter = null;
        System.out.println(domainName + " :-> Query :-> " + query);
        System.out.println(domainName + " :-> Actual Data FileName :-> " + fileName);

        Connection con = null;
        Statement stmt = null;

        try {

            con = this.hiveJdbcManager.getHiveJdbcConnection();
            stmt = con.createStatement();

            bufferedWriter = FileManager.getFileWriter(fileName);

            ResultSet res = stmt.executeQuery(query);
            ResultSetMetaData resMetadata = res.getMetaData();

            while (res.next()) {

                String resultRow = this.getResultRow(res, resMetadata, delimeter);
                FileManager.write(bufferedWriter, true, resultRow);
                System.out.println(domainName + " :-> RESULTS :-> " + resultRow);
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return "";
        } catch (IOException e) {
            e.printStackTrace();
            return "";
        } finally {
            try {
                stmt.close();
                con.close();
                FileManager.close(bufferedWriter);
            } catch (IOException e) {
                e.printStackTrace();
                return "";
            } catch (SQLException e) {
                e.printStackTrace();
                return "";
            }
        }

        return fileName;
    }

    /**
     *
     * @param query
     * @param domainName
     * @return boolean
     *
     */
    public boolean execute(String query, String domainName) {

        boolean res = false;

        Connection con = null;
        Statement stmt = null;

        try {

            con = this.hiveJdbcManager.getHiveJdbcConnection();
            stmt = con.createStatement();
            System.out.println(domainName + " :-> QUERY :-> " + query);
            res = stmt.execute(query);

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                stmt.close();
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
        }

        return res;
    }

    public boolean loadDataFromHDFSPath(String fileName, String tableName, String domainName) {

        Path hdfsTestDataSource = new Path(fileName);

        String query = "load data inpath '" + hdfsTestDataSource + "' into table " + tableName;

        return this.execute(query, domainName);
    }

    public boolean loadDataFromLocalPath(String fileName, String tableName, String domainName) {

        String query = "load data local inpath '" + fileName + "' into table " + tableName;

        return this.execute(query, domainName);
    }

    public boolean dropPartition(String tableName, String partitionString, String domainName) {

        String query = "alter table " + tableName + " drop if exists partition ( " + partitionString + " );";

        return this.execute(query, domainName);
    }

    private String getResultRow(ResultSet res, ResultSetMetaData resMetadata, String delimeter) throws SQLException {

        int columns = resMetadata.getColumnCount();

        StringBuffer result = new StringBuffer();

        for(int i=1; i<=columns; i++) {
            int columnType = resMetadata.getColumnType(i);

            switch(columnType) {

                case Types.VARCHAR:
                    result.append(res.getString(i) + delimeter);
                    break;

                case Types.INTEGER:
                case Types.SMALLINT:
                case Types.BIGINT:
                    result.append(res.getInt(i) + delimeter);
                    break;

                case Types.FLOAT:
                case Types.DECIMAL:
                case Types.DOUBLE:
                    result.append(res.getDouble(i) + delimeter);
                    break;

                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    result.append(res.getDate(i) + delimeter);
                    break;
            }
        }

        String resultString = result.toString();
        return resultString.substring(0, resultString.lastIndexOf(delimeter));
    }

    private Properties loadWorkflowProperties(String jobPropertiesFile) {

        Properties props = new Properties();

        InputStream is = ClassLoader.getSystemResourceAsStream(jobPropertiesFile);
        try {
            props.load(is);
        }
        catch (IOException e) {

            e.printStackTrace();
        }

        return props;
    }
}
