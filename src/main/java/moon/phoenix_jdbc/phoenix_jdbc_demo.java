package moon.phoenix_jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;


public class phoenix_jdbc_demo {
    //定义驱动类
    private static final String phoenix_driver = "org.apache.phoenix.jdbc.PhoenixDriver";
    //定义连接对象, ,返回结果集
    private static Connection conn =null; 
    private static PreparedStatement ps =null;
    private static ResultSet rs = null;
    
    public static void main(String[] args) {
        try {
            //加载驱动
            Class.forName(phoenix_driver);
            //获取连接
            Properties pro = new Properties();
            pro.setProperty("phoenix.schema.isNamespaceMappingEnabled","true");
            conn = DriverManager.getConnection("jdbc:phoenix:master,slave1,slave2:2181", pro);
            System.out.println("连接成功");
            
            //定义sql语句
            String sql = "select * from MY_TABLE";
            //发送sql语句
            ps = conn.prepareStatement(sql);
            //执行查询
            rs = ps.executeQuery();
            //取值
            while (rs.next()){
                System.out.println(rs.getString(1)+"\t"+rs.getString(2)
                +"\t"+rs.getString(3));
            }
        	} catch (ClassNotFoundException e) {
        		e.printStackTrace();
        	} catch (SQLException e) {
        		e.printStackTrace();
        	} finally {
        		
            //关闭连接
            try {
            	if (rs!=null){
                    rs.close();
                }
                if (ps!=null){
                    ps.close();
                }
                if (conn!=null){
                    conn.close();
                }
            } catch (SQLException e) {
                //do nothing
            } 
    }
    }
}
