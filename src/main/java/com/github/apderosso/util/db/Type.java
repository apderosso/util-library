package com.github.apderosso.util.db;

/**
 * List of supported database types.
 */
public enum Type {

    MSSQL("Microsoft SQL Server", "com.microsoft.sqlserver.jdbc.SQLServerDriver", "com.microsoft.sqlserver.jdbc.SQLServerDataSource"),
    MSSQL_JTDS("Microsoft SQL Server (jTDS)", "net.sourceforge.jtds.jdbc.Driver", "net.sourceforge.jtds.jdbcx.JtdsDataSource"),
    MYSQL("MySQL", "com.mysql.cj.jdbc.Driver", "com.mysql.cj.jdbc.MysqlDataSource");

    private final String name;
    private final String driverClass;
    private final String dataSourceClass;

    Type(final String name, final String driverClass, final String dataSourceClass) {
        this.name = name;
        this.driverClass = driverClass;
        this.dataSourceClass = dataSourceClass;
    }

    /**
     * Gets the database type name.
     *
     * @return database type name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the class for the DataSource for this database type.
     *
     * @return String class name
     */
    public String getDataSourceClass() {
        return dataSourceClass;
    }


    /**
     * Gets the class for the Driver for this database type.
     *
     * @return String class name
     */
    public String getDriverClass() {
        return driverClass;
    }

}
