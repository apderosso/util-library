package com.github.apderosso.util.db;

import com.github.apderosso.util.dbutils.CustomBeanProcessor;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;

/**
 * Represents a SQL database. Connections can be pooled if necessary.
 */
public class SqlDatabase {

    private static final Logger L = LoggerFactory.getLogger(SqlDatabase.class);

    private final Type type;
    private DataSource dataSource;
    private String jdbcUrl;
    private String user;
    private String password;
    private boolean useDataSource = true;
    private boolean useUsernamePassword = false;
    private boolean connectionPooled = false;

    /**
     * Default constructor, this should be used by default.
     *
     * @param type       database {@code Type}
     * @param dataSource configured {@code DataSource} to use
     */
    public SqlDatabase(final Type type, final DataSource dataSource) {
        this.type = type;
        this.dataSource = dataSource;
        useDataSource = true;
    }

    /**
     * Constructor with DataSource, user, and password.
     *
     * @param type       database {@code Type}
     * @param dataSource configured
     * @param user       database username
     * @param password   database password
     */
    public SqlDatabase(final Type type, final DataSource dataSource, final String user, final String password) {
        this(type, dataSource);
        this.user = user;
        this.password = password;
        useUsernamePassword = true;
    }

    /**
     * Constructor using a JDBC url. Using a DataSource should be preferred over the JDBC url.
     *
     * @param type    database {@code Type}
     * @param jdbcUrl JDBC connection URL
     */
    public SqlDatabase(final Type type, final String jdbcUrl) {
        this.type = type;
        this.jdbcUrl = jdbcUrl;
        useDataSource = false;
        initDriver();
    }

    /**
     * Constructor using a JDBC url, username, and password. Using a DataSource should be preferred over the JDBC url.
     *
     * @param type     database {@code Type}
     * @param jdbcUrl  JDBC connection URL
     * @param user     database username
     * @param password database password
     */
    public SqlDatabase(final Type type, final String jdbcUrl, final String user, final String password) {
        this(type, jdbcUrl);
        this.user = user;
        this.password = password;
        useUsernamePassword = true;
    }

    private void initDriver() {
        try {
            Class.forName(type.getDriverClass()).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            L.error("Unable to initialize driver for " + type.getName(), e);
        }
    }

    /**
     * Gets a connection th this SQL database.
     *
     * @return connection to database
     * @throws SQLException if unable to get a connection
     */
    public Connection getConnection() throws SQLException {
        final Connection con;
        if (useDataSource) {
            if (useUsernamePassword) {
                con = dataSource.getConnection(user, password);
            } else {
                con = dataSource.getConnection();
            }
        } else {
            if (useUsernamePassword) {
                con = DriverManager.getConnection(jdbcUrl);
            } else {
                con = DriverManager.getConnection(jdbcUrl, user, password);
            }
        }
        return con;
    }

    /**
     * Starts up a connection pool to this SQL database.
     */
    public void startConnectionPool() {
        startConnectionPool(null);
    }

    /**
     * Starts up a connection pool to this SQL database using a max pool size.
     *
     * @param maxPoolSize max connections for the db pool
     */
    public void startConnectionPool(final Integer maxPoolSize) {
        final ConnectionPool builder = new ConnectionPool();
        if (useDataSource) {
            builder.setDataSource(dataSource);
        } else {
            builder.setJdbcUrl(jdbcUrl);
        }
        if (useUsernamePassword) {
            builder.setUserAndPassword(user, password);
        }
        if (maxPoolSize != null) {
            builder.setMaxPoolSize(maxPoolSize);
        }
        dataSource = builder.build();
        connectionPooled = true;
    }

    /**
     * Shuts down the database connection pool.
     */
    public void shutdown() {
        if (connectionPooled) {
            ConnectionPool.shutdown(dataSource);
        }
    }

    /**
     * Returns the connection to the pool.
     *
     * @param connection connection to return to the pool
     */
    public static void returnConnection(final Connection connection) {
        DbUtils.closeQuietly(connection);
    }

    /**
     * Closes a {@code Statement} squelching any errors.
     *
     * @param statement Statement to close
     */
    public static void closeStatement(final Statement statement) {
        DbUtils.closeQuietly(statement);
    }

    /**
     * Closes a {@code ResultSet} squelching any errors.
     *
     * @param resultSet ResultSet to close
     */
    public static void closeResultSet(final ResultSet resultSet) {
        DbUtils.closeQuietly(resultSet);
    }

    /**
     * Returns a {@code Connection} to the pool and closes the {@code Statement} and {@code ResultSet}. Any of the params can be null.
     *
     * @param connection Connection to close
     * @param statement  Statement to close
     * @param resultSet  ResultSet to close
     */
    public static void returnConnection(final Connection connection, final Statement statement, final ResultSet resultSet) {
        DbUtils.closeQuietly(connection, statement, resultSet);
    }

    /**
     * Gets a bean list handler for the specified class. This handler takes care of underscores in column names.
     *
     * @param <T>   type to be returned in the list
     * @param clazz {@code Class} of T
     * @return a {@code BeanListHandler} for type T
     */
    public static <T> BeanListHandler<T> getAAOSBeanListHandler(final Class<T> clazz) {
        return new BeanListHandler<>(clazz, new BasicRowProcessor(new CustomBeanProcessor()));
    }

    /**
     * Check whether the connections are in a connection pool.
     *
     * @return boolean whether connections pool is running
     */
    public boolean isConnectionPooled() {
        return connectionPooled;
    }

}
