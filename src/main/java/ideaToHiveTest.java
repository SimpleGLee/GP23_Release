import java.sql.*;

/**
 * Write By SimpleLee
 * On 2019-九月-星期四
 * 17-41
 * Idea连接Hive测试
 */
public class ideaToHiveTest {
    //    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
    //jdbc核心驱动类
    //private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException {
        try {
            //加载jdbc核心驱动类
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
//        Connection conn = DriverManager.getConnection("jdbc:hive://192.168.145.130:10000/test","root","root");
        //建立连接
        Connection conn = DriverManager.getConnection("jdbc:hive://192.168.237.129:10000/test1","root","zsh...");
        //创建执行器
        Statement sta = conn.createStatement();

        String tableName = "jdbctest";
        //定义执行sql语句
        sta.execute("use test1");
        sta.execute("drop table if exists " + tableName);
        sta.execute("create table " + tableName + " (id int,name string)");
        System.out.println("success!");

        //定义执行结果集和sql语句
        ResultSet res = sta.executeQuery("show tables '" + tableName + "'");
        if(res.next()){
            System.out.println(res.getString(1));
        }
    }
}
