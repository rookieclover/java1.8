import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * 数据库连接对象管理类
 * 
 * @说明
 * @author cuisuqiang
 * @version 1.0
 * @since
 */
public class JdbcIdentity {
	private static final String url = "jdbc:mysql://192.168.3.17:3306/test";
	private static final String username = "tongbao";
	private static final String userpass = "mainone%test123";

	public static void main(String[] args) {
		for(int i =0 ; i < 1;i ++)
		{
		System.out.println(catchTest());
		}
	}

	public static boolean catchTest() {
		try {
			int i = 10 / 2; // 抛出 Exception，后续处理被拒绝
			System.out.println("i vaule is : " + i);
			return true; // Exception 已经抛出，没有获得被执行的机会
		} catch (Exception e) {
			System.out.println(" -- Exception --");
			return catchMethod(); // Exception 抛出，获得了调用方法并返回方法值的机会
		}
		finally{
			 finallyMethod();  // Exception 抛出，finally 代码块将在 catch 执行 return 之前被执行
		}
	}

	// catch 后续处理工作
	public static boolean catchMethod() {
		System.out.print("call catchMethod and return  --->>  ");
		return false;
	}

	// finally后续处理工作
	public static void finallyMethod() {
		System.out.println();
		System.out.print("call finallyMethod and do something  --->>  ");
	}

	public static Connection getConnection() {
		Connection conn = null;
		try {
			com.mysql.jdbc.Driver driver = new com.mysql.jdbc.Driver();
			Properties properties = new Properties();
			properties.put("user", username);
			properties.put("password", userpass);
			conn = driver.connect(url, properties);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static void test() throws SQLException {
		com.mysql.jdbc.Driver dr = new com.mysql.jdbc.Driver();
		Properties p = new Properties();
		p.put("user", "tongbao");
		p.put("password", "mainone%test123");
		String url = "jdbc:mysql://192.168.3.17:3306/test";
		Connection con = dr.connect(url, p);
		String sql = "select ?,? from dual";
		PreparedStatement ps = con.prepareStatement(sql);
		ps.setInt(1, 999);
		ps.setInt(2, 1000);
		ps.execute();
		ResultSet rs = ps.getResultSet();

		if (rs.next()) {
			int a = rs.getInt(1);
			int b = rs.getInt(2);
			System.out.println(a + "_" + b);
		}
	}
}