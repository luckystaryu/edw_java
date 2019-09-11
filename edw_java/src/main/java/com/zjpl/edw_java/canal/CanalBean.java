package com.zjpl.edw_java.canal;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class CanalBean implements Serializable {
    //数据库
    private String database;
    //表
    private String table;
    //操作时间
    private Long executeTime;
    //操作类型INSERT =1;UPDATE
    //操作钱字段信息
    private int eventType;
    //ddl的sql
    private String ddlSql;
    //行数据
    private RowData rowData;

    public static class RowData {
        //操作前字段信息
        private Map<String, ColumnEntry> beforeColumns;
        private Map<String, ColumnEntry> afterColumns;

        public RowData() {
        }

        public RowData(Map<String, ColumnEntry> beforeColumns, Map<String, ColumnEntry> afterColumns) {
            this.beforeColumns = beforeColumns;
            this.afterColumns = afterColumns;
        }

        public static class ColumnEntry {
            //字段名称
            private String name;
            //是否主键
            private Boolean isKey;
            //是否修改
            private Boolean updated;
            //是否为空
            private Boolean isNull;
            //字段的值
            private String value;

            public ColumnEntry() {
            }

            public ColumnEntry(String name, Boolean isKey, Boolean updated, Boolean isNull, String value) {
                this.name = name;
                this.isKey = isKey;
                this.updated = updated;
                this.isNull = isNull;
                this.value = value;
            }

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

            public Boolean getKey() {
                return isKey;
            }

            public void setKey(Boolean key) {
                isKey = key;
            }

            public Boolean getUpdated() {
                return updated;
            }

            public void setUpdated(Boolean updated) {
                this.updated = updated;
            }

            public Boolean getNull() {
                return isNull;
            }

            public void setNull(Boolean aNull) {
                isNull = aNull;
            }

            public String getValue() {
                return value;
            }

            public void setValue(String value) {
                this.value = value;
            }
        }
    }

    public CanalBean(String database, String table, long executeTime, int eventType, String ddlSql) {
        this.database = database;
        this.table = table;
        this.executeTime = executeTime;
        this.eventType = eventType;
        this.ddlSql = ddlSql;
    }
    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(Long executeTime) {
        this.executeTime = executeTime;
    }

    public int getEventType() {
        return eventType;
    }

    public void setEventType(int eventType) {
        this.eventType = eventType;
    }

    public String getDdlSql() {
        return ddlSql;
    }

    public void setDdlSql(String ddlSql) {
        this.ddlSql = ddlSql;
    }

    public RowData getRowData() {
        return rowData;
    }

    public void setRowData(RowData rowData) {
        this.rowData = rowData;
    }

}
