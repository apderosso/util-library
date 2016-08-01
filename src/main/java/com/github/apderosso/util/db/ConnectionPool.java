package com.github.apderosso.util.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

/**
 * Class to handle connection pooling.
 */
public class ConnectionPool {

    private DataSource dataSource;
    private String jdbcUrl;
    private String user;
    private String password;
    private int maxPoolSize = 10;

    /**
     * Default constructor
     */
    public ConnectionPool() {
    }

    /**
     * Sets the DataSource to be pooled.
     *
     * @param dataSource DataSource to pool
     * @return builder type return to chain config methods
     */
    public ConnectionPool setDataSource(final DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }

    /**
     * Sets the JDBC url to use in the pool.
     *
     * @param jdbcUrl JDBC url
     * @return builder type return to chain config methods
     */
    public ConnectionPool setJdbcUrl(final String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    /**
     * Sets the username and password to use in the connection pool. This is unnecessary if this info is already set in the DataSource or JDBC url.
     *
     * @param user     database username
     * @param password database password
     * @return builder type return to chain config methods
     */
    public ConnectionPool setUserAndPassword(final String user, final String password) {
        this.user = user;
        this.password = password;
        return this;
    }

    /**
     * Sets the max number of connections to use in the pool.
     *
     * @param maxPoolSize max pool size
     * @return builder type return to chain config methods
     */
    public ConnectionPool setMaxPoolSize(final int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        return this;
    }

    /**
     * Builds the pooled DataSource.
     *
     * @return pooled DataSource
     */
    public DataSource build() {
        if (dataSource == null && jdbcUrl == null) {
            throw new RuntimeException("Either DataSource of JDBC url must be set to use a connection pool");
        }
        final HikariConfig config = new HikariConfig();
        if (dataSource != null) {
            config.setDataSource(dataSource);
        } else {
            config.setJdbcUrl(jdbcUrl);
        }
        if (user != null && password != null) {
            config.setUsername(user);
            config.setPassword(password);
        }
        config.setMaximumPoolSize(maxPoolSize);
        return new HikariDataSource(config);
    }

    /**
     * Shuts down the connection pool.
     *
     * @param dataSource DataSource to shutdown
     */
    public static void shutdown(final DataSource dataSource) {
        ((HikariDataSource) dataSource).close();
    }

}
