package com.zjpl.edw.page;

import java.io.Serializable;
import java.util.List;

/**
 * 提供列页，分页参数
 * @param <T>
 */
public class BootstrapTablePaginationVo<T> implements Serializable {
    private static final long serialVersionUID =1L;
    /**
     * 记录总数
     */
    private long total;
    /**
     * 记录总数
     */
    private List<T> rows;

    public BootstrapTablePaginationVo(){

    }
    public BootstrapTablePaginationVo(long total, List<T> rows) {
        this.total = total;
        this.rows = rows;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public List<T> getRows() {
        return rows;
    }

    public void setRows(List<T> rows) {
        this.rows = rows;
    }
}
