package com.github.apderosso.util.dbutils;

import org.apache.commons.dbutils.BeanProcessor;

import java.beans.PropertyDescriptor;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * Bean processor that gets rid of underscores in column names.
 *
 * @author Austin DeRosso
 */
public class CustomBeanProcessor extends BeanProcessor {

    @Override
    protected int[] mapColumnsToProperties(final ResultSetMetaData rsmd, final PropertyDescriptor[] props) throws SQLException {
        final int cols = rsmd.getColumnCount();
        final int[] columnToProperty = new int[cols + 1];
        Arrays.fill(columnToProperty, PROPERTY_NOT_FOUND);

        for (int col = 1; col <= cols; col++) {
            String columnName = rsmd.getColumnLabel(col);
            if (null == columnName || 0 == columnName.length()) {
                columnName = rsmd.getColumnName(col);
            }

            // Strip out bad characters
            columnName = columnName.replace("_", "");

            for (int i = 0; i < props.length; i++) {
                if (columnName.equalsIgnoreCase(props[i].getName())) {
                    columnToProperty[col] = i;
                    break;
                }
            }
        }

        return columnToProperty;
    }

}
