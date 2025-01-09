package org.example;

import lombok.Setter;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;

public class SimpleDb implements AutoCloseable {
    private Connection connection;

    @Setter
    private boolean devMode = false;

    public SimpleDb(String host, String username, String password, String database) {
        try {
            connection = DriverManager.getConnection(
                    "jdbc:mysql://" + host + "/" + database, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void logSql(String sql, Object[] params) {
        if (devMode) {
            System.out.println("Executing SQL: " + sql);
            System.out.println("Parameters: " + Arrays.toString(params));
        }
    }

    public Sql genSql() {
        return new Sql(this);
    }

    public void run(String sql, Object... params) {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            setParameters(stmt, params);
            stmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public long update(String sql, Object[] params) {
        logSql(sql, params);

        try {
            if (connection.getAutoCommit()) {
                connection.setAutoCommit(false);
            }

            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                setParameters(stmt, params);

                long affectedRows = stmt.executeUpdate();

                return affectedRows;

            } catch (SQLException e) {
                e.printStackTrace();
                connection.rollback();
                return 0;
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return 0;
        }
    }

    public List<Map<String, Object>> selectRows(String sql, Object[] params) {
        List<Map<String, Object>> result = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            setParameters(stmt, params);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                result.add(convertRow(rs));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public <T> List<T> selectRows(String sql, Object[] params, Class<T> clazz) {
        List<T> result = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            setParameters(stmt, params);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                T instance = clazz.getDeclaredConstructor().newInstance();

                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    String columnName = rs.getMetaData().getColumnName(i);
                    Object value = rs.getObject(i);

                    String setterName = "set" + columnName.substring(0, 1).toUpperCase() + columnName.substring(1);
                    try {
                        Method setter = clazz.getMethod(setterName, value.getClass());
                        setter.invoke(instance, value);
                    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
                result.add(instance);
            }
        } catch (SQLException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return result;
    }

    public Map<String, Object> selectRow(String sql, Object[] params) {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            setParameters(stmt, params);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return convertRow(rs);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public LocalDateTime selectDatetime(String sql, Object[] params) {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            setParameters(stmt, params);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getTimestamp(1).toLocalDateTime();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Long selectLong(String sql, Object[] params) {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            System.out.println(sql);
            setParameters(stmt, params);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String selectString(String sql, Object[] params) {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            setParameters(stmt, params);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Boolean selectBoolean(String sql, Object[] params) {
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            setParameters(stmt, params);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getBoolean(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void setParameters(PreparedStatement stmt, Object[] params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            stmt.setObject(i + 1, params[i]);
        }
    }

    private Map<String, Object> convertRow(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        Map<String, Object> row = new HashMap<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            row.put(metaData.getColumnName(i), rs.getObject(i));
        }
        return row;
    }

    @Override
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void startTransaction() {
        try {
            if (connection != null && connection.getAutoCommit()) {
                connection.setAutoCommit(false);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void commit() {
        try {
            if (connection != null && !connection.getAutoCommit()) {
                connection.commit();
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void rollback() {
        try {
            if (connection != null) {
                connection.rollback();
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}