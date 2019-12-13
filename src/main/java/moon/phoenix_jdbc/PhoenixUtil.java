package moon.phoenix_jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class PhoenixUtil {
	 //定义驱动类
    private static final String phoenix_driver = "org.apache.phoenix.jdbc.PhoenixDriver";
    //定义连接对象, ,返回结果集
    private static Connection conn =null; 
    private static PreparedStatement ps =null;
    private static ResultSet rs = null;
    
    //获取连接
    public static void getConnection() {    	
        try {
        	//加载驱动
			Class.forName(phoenix_driver);
			Properties pro = new Properties();
            pro.setProperty("phoenix.schema.isNamespaceMappingEnabled","true");
			conn = DriverManager.getConnection("jdbc:phoenix:master,slave1,slave2:2181", pro);
            System.out.println("连接成功");	
            
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}catch (SQLException e) {
				e.printStackTrace();
			}
    	}
    
    //关闭连接
    public static void closeConnection() {
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
    
    //建表
    public static void creatTable() {
    	try {
    		getConnection();
    		String sql = "";
    		ps = conn.prepareStatement(sql);
    		//执行查询
    		 ps.executeUpdate();
    		 System.out.println("完成");
    	} catch (SQLException e) {
    		e.printStackTrace();
    	}
    
    }
   

}
