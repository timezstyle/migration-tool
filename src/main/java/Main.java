import model.Column;
import model.DbInfo;
import model.Table;
import org.apache.commons.io.FileUtils;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author timling
 * @since 2015/3/17
 */
public class Main {
    private static String splitTable = "__TO__";
    private static String splitColumn = ",";
    private static int readRecordLimit = 1000;

    private static DbInfo fromDb = new DbInfo("url", "userName", "password");
    private static DbInfo toDb = new DbInfo("url", "userName", "password");

    public static void main(String[] args) {

        File directory = new File("migrateCsv");
        try {
            // load csv to Tables
            List<Table> tables = loadCsv(directory);

            for (Table table : tables) {
                migrate(table);
            }
            System.out.println("done !!");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void migrate(Table table) {
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;

        try {

            conn = DriverManager.getConnection(fromDb.url, fromDb.username, fromDb.password);

            st = conn.createStatement();
            rs = st.executeQuery(table.toSelectSql());
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();

            int readRecordCount = 0;
            List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
            while (rs.next()) {
                Map<String, Object> columns = new LinkedHashMap<String, Object>();

                for (int i = 1; i <= columnCount; i++) {
                    columns.put(meta.getColumnLabel(i), rs.getObject(i));
                }

                rows.add(columns);
                if (++readRecordCount % readRecordLimit == 0) {
                    // batch insert
                    batchInsert(table, rows);
                }
            }
            if (!rows.isEmpty()) {
                // batch insert
                batchInsert(table, rows);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException e) {
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                }
            }
        }
    }

    private static void batchInsert(Table table, List<Map<String, Object>> rows) throws SQLException {
        DBI dbi = new DBI(toDb.url, toDb.username, toDb.password);
        Handle h = null;
        try {
            h = dbi.open();
            PreparedBatch preparedBatch = h.prepareBatch(table.toInsertSql());
            for (Map<String, Object> row : rows) {
                preparedBatch.add(row);
            }
            preparedBatch.execute();
            // clear rows after batch insert
            rows.clear();
        } finally {
            if (h != null) {
                try {
                    h.close();
                } catch (Exception e) {
                }
            }
        }
    }

    private static List<Table> loadCsv(File directory) throws IOException {
        List<Table> tables = new ArrayList<Table>();
        FileUtils.forceMkdir(directory);
        File[] files = directory.listFiles();
        for (File file : files) {
            if (file.isFile() && file.getName().contains(splitTable)) {
                String[] tableArray = file.getName().replace(".csv", "").split(splitTable);
                Table table = new Table();
                table.from = tableArray[0];
                table.to = tableArray[1];
                List<String> list = FileUtils.readLines(file);
                for (String str : list) {
                    String[] columnArray = str.split(splitColumn);
                    Column column = new Column();
                    column.from = columnArray[0];
                    column.to = columnArray[1];
                    table.columns.add(column);
                }
                tables.add(table);
            }
        }
        return tables;
    }
}
