package io.vertx.source;

/**
 * Created by Administrator on 2018/2/2.
 * 
 */
public enum QuerySql {
    CREATE_PAGES_TABLE,
    ALL_PAGES,
    GET_PAGE,
    CREATE_PAGE,
    SAVE_PAGE,
    DELETE_PAGE;

    private String sql;

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String sql() {
        return this.sql;
    }
}
