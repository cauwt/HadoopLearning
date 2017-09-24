package hive;

import java.sql.*;
import java.util.ResourceBundle;

/**
 * Created by yachao on 17/9/24.
 */
public class HiveExample {
    private Connection conn;

    public HiveConfig getHiveConfig() {
        HiveConfig hiveConfig = new HiveConfig();
        ResourceBundle rb = ResourceBundle.getBundle("hive");

        hiveConfig.setDriver((String) rb.getObject("driver"));
        hiveConfig.setUrl((String) rb.getObject("url"));
        hiveConfig.setUsername((String) rb.getObject("username"));
        hiveConfig.setPassword((String) rb.getObject("password"));

        return hiveConfig;
    }

    public void init() {
        HiveConfig hiveConfig = getHiveConfig();

        try {
            Class.forName(hiveConfig.getDriver());
            conn = DriverManager.getConnection(hiveConfig.getUrl(), hiveConfig.getUsername(), hiveConfig.getPassword());
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public void showTables() throws SQLException {
        init();

        PreparedStatement preparedStatement = conn.prepareStatement("show tables");
        ResultSet rs = preparedStatement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getObject(1));
        }

        preparedStatement.close();
        conn.close();
    }

    public void selectDiffProvinceConsumeResult() throws SQLException {
        init();

        StringBuffer hsql = new StringBuffer("");
        hsql.append("select province, sum(price) as totalPrice ");
        hsql.append("from record join user_dimension on record.uid=user_dimension.uid ");
        hsql.append("group by province ");
        hsql.append("order by totalPrice desc");
        PreparedStatement preparedStatement = conn.prepareStatement(hsql.toString());
        ResultSet rs = preparedStatement.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getString(1) + "  " + rs.getInt(2));
        }

        preparedStatement.close();
        conn.close();
    }

    public static void main(String[] args) {
        try {
//            new HiveExample().showTables();
            new HiveExample().selectDiffProvinceConsumeResult();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private class HiveConfig {
        private String driver;
        private String url;
        private String username;
        private String password;

        public String getDriver() {
            return driver;
        }

        public void setDriver(String driver) {
            this.driver = driver;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        @Override
        public String toString() {
            return "HiveConfig{" +
                    "driver='" + driver + '\'' +
                    ", url='" + url + '\'' +
                    ", username='" + username + '\'' +
                    ", password='" + password + '\'' +
                    '}';
        }
    }
}