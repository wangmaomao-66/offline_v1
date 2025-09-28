import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestHiveConnection {
    public static void main(String[] args) {
        // Hive 连接配置
        String hiveHost = "cdh03"; // HiveServer2 IP
        String hivePort = "10000";         // HiveServer2 端口
        String hiveDb = "realtime_v1_hive";         // Hive 数据库
        String hiveUser = "";              // Hive 用户（无认证可空）
        String hivePass = "";              // Hive 密码（无认证可空）

        String jdbcUrl = "jdbc:hive2://" + hiveHost + ":" + hivePort + "/" + hiveDb;

        try {
            // 加载 Hive JDBC 驱动
            Class.forName("org.apache.hive.jdbc.HiveDriver");

            // 建立连接
            Connection conn = DriverManager.getConnection(jdbcUrl, hiveUser, hivePass);
            System.out.println("JDBC 连接成功！");

            // 测试查询 Hive 表列表
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW TABLES");

            System.out.println("Hive 数据库表列表：");
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }

            rs.close();
            stmt.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}