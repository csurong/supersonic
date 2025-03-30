package com.tencent.supersonic.headless.core.adaptor.db;

import com.tencent.supersonic.common.pojo.Constants;
import com.tencent.supersonic.common.pojo.enums.TimeDimensionEnum;
import com.tencent.supersonic.headless.api.pojo.DBColumn;
import com.tencent.supersonic.headless.api.pojo.enums.FieldType;
import com.tencent.supersonic.headless.core.pojo.ConnectInfo;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class SqlServerAdaptor extends BaseDbAdaptor {

    @Override
    public String getDateFormat(String dateType, String dateFormat, String column) {
        if (dateFormat.equalsIgnoreCase(Constants.DAY_FORMAT_INT)) {
            if (TimeDimensionEnum.MONTH.name().equalsIgnoreCase(dateType)) {
                return String.format("FORMAT(TRY_CONVERT(date, CONVERT(varchar, %s)), 'yyyy-MM')", column);
            } else if (TimeDimensionEnum.WEEK.name().equalsIgnoreCase(dateType)) {
                return String.format("FORMAT(DATEADD(day, -(DATEPART(weekday, TRY_CONVERT(date, CONVERT(varchar, %s))) - 2), TRY_CONVERT(date, CONVERT(varchar, %s))), 'yyyy-MM-dd')", column, column);
            } else {
                return String.format("FORMAT(TRY_CONVERT(date, CONVERT(varchar, %s)), 'yyyy-MM-dd')", column);
            }
        } else if (dateFormat.equalsIgnoreCase(Constants.DAY_FORMAT)) {
            if (TimeDimensionEnum.MONTH.name().equalsIgnoreCase(dateType)) {
                return String.format("FORMAT(%s, 'yyyy-MM')", column);
            } else if (TimeDimensionEnum.WEEK.name().equalsIgnoreCase(dateType)) {
                return String.format("FORMAT(DATEADD(day, -(DATEPART(weekday, %s) - 2), %s), 'yyyy-MM-dd')", column, column);
            } else {
                return column;
            }
        }
        return column;
    }

    @Override
    public String rewriteSql(String sql) {
        if (sql == null) {
            return null;
        }
        // 替换反引号为方括号，这是SQL Server的标识符引用方式
        return sql.replaceAll("`([^`]*)`", "[$1]");
    }

    @Override
    protected ResultSet getResultSet(String schemaName, DatabaseMetaData metaData) throws SQLException {
        // SQL Server中，schema通常是dbo，但也可以是其他值
        return metaData.getTables(null, schemaName, null, new String[] {"TABLE", "VIEW"});
    }

    @Override
    public FieldType classifyColumnType(String typeName) {
        switch (typeName.toUpperCase()) {
            case "INT":
            case "BIGINT":
            case "SMALLINT":
            case "TINYINT":
            case "DECIMAL":
            case "NUMERIC":
            case "MONEY":
            case "SMALLMONEY":
            case "FLOAT":
            case "REAL":
                return FieldType.measure;
            case "DATE":
            case "DATETIME":
            case "DATETIME2":
            case "SMALLDATETIME":
            case "TIME":
            case "DATETIMEOFFSET":
                return FieldType.time;
            default:
                return FieldType.categorical;
        }
    }

    @Override
    public List<String> getCatalogs(ConnectInfo connectInfo) {
        List<String> catalogs = new ArrayList<>();
        try (Connection connection = getConnection(connectInfo)) {
            // 方法1：使用DatabaseMetaData获取所有数据库
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = metaData.getCatalogs()) {
                while (rs.next()) {
                    String catalogName = rs.getString("TABLE_CAT");
                    // 过滤掉系统数据库
                    if (!isSystemDatabase(catalogName)) {
                        catalogs.add(catalogName);
                    }
                }
            }
            
            // 如果使用DatabaseMetaData没有获取到结果，尝试使用SQL查询
            if (catalogs.isEmpty()) {
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT name FROM sys.databases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb') ORDER BY name")) {
                    while (rs.next()) {
                        catalogs.add(rs.getString("name"));
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get catalogs from SQL Server", e);
        }
        return catalogs;
    }
    
    /**
     * 判断是否为SQL Server系统数据库
     */
    private boolean isSystemDatabase(String databaseName) {
        return "master".equalsIgnoreCase(databaseName) ||
               "tempdb".equalsIgnoreCase(databaseName) ||
               "model".equalsIgnoreCase(databaseName) ||
               "msdb".equalsIgnoreCase(databaseName);
    }

    @Override
    public List<DBColumn> getColumns(ConnectInfo connectInfo, String catalog, String schemaName,
            String tableName) throws SQLException {
        List<DBColumn> columns = new ArrayList<>();
        
        // 如果没有指定必要参数，则返回空列表
        if (catalog == null || catalog.isEmpty() || schemaName == null || schemaName.isEmpty() 
                || tableName == null || tableName.isEmpty()) {
            return columns;
        }
        
        try (Connection connection = getConnection(connectInfo)) {
            // 首先尝试切换到指定的数据库
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("USE [" + catalog + "]");
            }
            
            // 使用SQL查询获取列信息，避免使用可能导致问题的DatabaseMetaData
            String sql = "SELECT " +
                    "c.COLUMN_NAME, " +
                    "c.DATA_TYPE, " +
                    "c.COLUMN_DEFAULT, " +
                    "c.IS_NULLABLE, " +
                    "ep.value AS COLUMN_COMMENT, " +
                    "CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES' ELSE 'NO' END AS IS_PRIMARY_KEY " +
                    "FROM INFORMATION_SCHEMA.COLUMNS c " +
                    "LEFT JOIN (" +
                    "    SELECT ku.TABLE_CATALOG, ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME " +
                    "    FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc " +
                    "    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ku " +
                    "    ON tc.CONSTRAINT_TYPE = 'PRIMARY KEY' " +
                    "    AND tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME " +
                    ") pk " +
                    "ON c.TABLE_CATALOG = pk.TABLE_CATALOG " +
                    "AND c.TABLE_SCHEMA = pk.TABLE_SCHEMA " +
                    "AND c.TABLE_NAME = pk.TABLE_NAME " +
                    "AND c.COLUMN_NAME = pk.COLUMN_NAME " +
                    "LEFT JOIN sys.tables t ON t.name = c.TABLE_NAME AND SCHEMA_NAME(t.schema_id) = c.TABLE_SCHEMA " +
                    "LEFT JOIN sys.columns sc ON sc.object_id = t.object_id AND sc.name = c.COLUMN_NAME " +
                    "LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id AND ep.minor_id = sc.column_id AND ep.name = 'MS_Description' " +
                    "WHERE c.TABLE_CATALOG = ? " +
                    "AND c.TABLE_SCHEMA = ? " +
                    "AND c.TABLE_NAME = ? " +
                    "ORDER BY c.ORDINAL_POSITION";
            
            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                pstmt.setString(1, catalog);
                pstmt.setString(2, schemaName);
                pstmt.setString(3, tableName);
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        DBColumn column = new DBColumn();
                        column.setColumnName(rs.getString("COLUMN_NAME"));
                        column.setDataType(rs.getString("DATA_TYPE"));
                        
                        // 设置字段类型
                        column.setFieldType(classifyColumnType(rs.getString("DATA_TYPE")));
                        
                        // 设置注释
                        String comment = rs.getString("COLUMN_COMMENT");
                        if (comment == null) {
                            // 如果没有注释，可以使用默认值或其他信息
                            comment = "";
                        }
                        column.setComment(comment);
                        
                        columns.add(column);
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get columns from SQL Server database: " + catalog + 
                    ", schema: " + schemaName + ", table: " + tableName, e);
            throw e;
        }
        
        return columns;
    }

    @Override
    public List<String> getDBs(ConnectInfo connectInfo, String catalog) throws SQLException {
        List<String> schemas = new ArrayList<>();
        
        // 如果没有指定catalog，则返回空列表
        if (catalog == null || catalog.isEmpty()) {
            return schemas;
        }
        
        try (Connection connection = getConnection(connectInfo)) {
            // 首先尝试切换到指定的数据库
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("USE [" + catalog + "]");
            }
            
            // 方法1：使用DatabaseMetaData获取所有schema
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = metaData.getSchemas(catalog, null)) {
                while (rs.next()) {
                    String schemaName = rs.getString("TABLE_SCHEM");
                    // 过滤掉系统schema
                    if (!isSystemSchema(schemaName)) {
                        schemas.add(schemaName);
                    }
                }
            }
            
            // 如果使用DatabaseMetaData没有获取到结果，尝试使用SQL查询
            if (schemas.isEmpty()) {
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery(
                             "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA " +
                             "WHERE SCHEMA_NAME NOT IN ('sys', 'information_schema', 'guest', 'INFORMATION_SCHEMA') " +
                             "ORDER BY SCHEMA_NAME")) {
                    while (rs.next()) {
                        schemas.add(rs.getString("SCHEMA_NAME"));
                    }
                }
            }
            
            // 如果仍然没有结果，至少添加默认的dbo schema
            if (schemas.isEmpty()) {
                schemas.add("dbo");
            }
        } catch (SQLException e) {
            log.error("Failed to get schemas from SQL Server database: " + catalog, e);
        }
        
        return schemas;
    }
    
    /**
     * 判断是否为SQL Server系统schema
     */
    private boolean isSystemSchema(String schemaName) {
        return "sys".equalsIgnoreCase(schemaName) ||
               "information_schema".equalsIgnoreCase(schemaName) ||
               "guest".equalsIgnoreCase(schemaName) ||
               "INFORMATION_SCHEMA".equalsIgnoreCase(schemaName);
    }

    @Override
    public List<String> getTables(ConnectInfo connectInfo, String catalog, String dbName) throws SQLException {
        List<String> tables = new ArrayList<>();
        
        // 如果没有指定catalog或dbName，则返回空列表
        if (catalog == null || catalog.isEmpty() || dbName == null || dbName.isEmpty()) {
            return tables;
        }
        
        try (Connection connection = getConnection(connectInfo)) {
            // 首先尝试切换到指定的数据库
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("USE [" + catalog + "]");
            }
            
            // 使用DatabaseMetaData获取表列表
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = metaData.getTables(catalog, dbName, null, new String[]{"TABLE", "VIEW"})) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    // 过滤掉系统表
                    if (!isSystemTable(tableName)) {
                        tables.add(tableName);
                    }
                }
            }
            
            // 如果使用DatabaseMetaData没有获取到结果，尝试使用SQL查询
            if (tables.isEmpty()) {
                try (Statement stmt = connection.createStatement();
                     ResultSet rs = stmt.executeQuery(
                             "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
                             "WHERE TABLE_SCHEMA = '" + dbName + "' " +
                             "AND TABLE_TYPE IN ('BASE TABLE', 'VIEW') " +
                             "ORDER BY TABLE_NAME")) {
                    while (rs.next()) {
                        String tableName = rs.getString("TABLE_NAME");
                        if (!isSystemTable(tableName)) {
                            tables.add(tableName);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get tables from SQL Server database: " + catalog + ", schema: " + dbName, e);
            throw e;
        }
        
        return tables;
    }

    /**
     * 判断是否为SQL Server系统表
     */
    private boolean isSystemTable(String tableName) {
        return tableName.startsWith("sys") || 
               tableName.startsWith("dt_") || 
               tableName.startsWith("MSreplication_") ||
               tableName.startsWith("spt_") ||
               tableName.startsWith("queue_messages_") ||
               tableName.equals("sysdiagrams");
    }
}
