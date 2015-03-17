package model;

import java.util.ArrayList;
import java.util.List;

/**
 * @author timling
 * @since 2015/3/17
 */
public class Table {
    public String from;
    public String to;
    public List<Column> columns = new ArrayList<Column>();

    public String toSelectSql() {
        StringBuilder sql = new StringBuilder("select ");
        appendColumnWithComma(sql, "from", null);
        sql.append(" from " + from);
        return sql.toString();
    }

    public String toInsertSql() {
        StringBuilder sql = new StringBuilder("insert into " + to + " (");
        appendColumnWithComma(sql, "to", null);
        sql.append(") values (");
        appendColumnWithComma(sql, "from", ":");
        sql.append(");");
        return sql.toString();
    }

    private void appendColumnWithComma(StringBuilder sql, String isFromColumn
            , String prefix) {
        int i = 0;
        for (Column column : columns) {
            if (i++ != 0) {
                sql.append(", ");
            }
            if (prefix != null) {
                sql.append(prefix);
            }
            if ("from".equals(isFromColumn)) {
                sql.append(column.from);
            } else {
                sql.append(column.to);
            }
        }
    }

    @Override
    public String toString() {
        return "Table{" +
                "from='" + from + '\'' +
                ", to='" + to + '\'' +
                ", columns=" + columns +
                '}';
    }
}
