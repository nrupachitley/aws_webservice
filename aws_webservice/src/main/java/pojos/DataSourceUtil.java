package pojos;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * @author Nrupa
 */
public class DataSourceUtil {

    private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";

    private static ComboPooledDataSource sqlConnPool;
    private static HConnection hbaseConn;
    private static ExecutorService executor = Executors.newFixedThreadPool(3);
    private static DataSourceUtil dsFactory = new DataSourceUtil();

    public enum DataSourceType {

        SQL_DATA_SOURCE, HBASE_DATA_SOURCE;
    }

    private DataSourceUtil() {
    }

    public static DataSourceUtil getInstance() {
        return dsFactory;
    }


    public static ExecutorService getHbaseExecutorSvc() {
        return executor;
    }

    public static ComboPooledDataSource getSQLConnectionPool(DataSourceType ds, String host, String port, String user,
                                                             String password, String database, int min, int max, int increment)
            throws PropertyVetoException, MasterNotRunningException, ZooKeeperConnectionException, IOException {

        if (sqlConnPool == null) {
            sqlConnPool = new ComboPooledDataSource();

            sqlConnPool.setDriverClass(MYSQL_DRIVER_CLASS);
            sqlConnPool.setJdbcUrl("jdbc:mysql://" + host + ":" + port + "/" + database);
            sqlConnPool.setUser(user);
            sqlConnPool.setPassword(password);
            sqlConnPool.setMinPoolSize(min);
            sqlConnPool.setAcquireIncrement(increment);
            sqlConnPool.setMaxPoolSize(max);
        }

        return sqlConnPool;
    }

}