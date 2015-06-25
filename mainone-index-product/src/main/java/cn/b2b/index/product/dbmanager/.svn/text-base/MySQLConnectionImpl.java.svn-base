package cn.b2b.index.product.dbmanager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class MySQLConnectionImpl {
    static final Logger LOG = Logger.getLogger("create");
    private String connurl;
    private String username;
    private String password;
    private Connection conn;
    
    public MySQLConnectionImpl(String connurl, String username, String password) {
        this.connurl = connurl;
        this.username = username;
        this.password = password;
    }

    public Connection openConnection() throws SQLException {
        try {
            if (conn != null && !conn.isClosed()) {
                return conn;
            }
        } catch (SQLException e) {

        }
        closeConnection();

        try {
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println(connurl);
            System.out.println(username);
            System.out.println(password);
            conn = DriverManager.getConnection(connurl, username, password);
        } catch (ClassNotFoundException e) {
            LOG.error("Cannot find mysql Driver:" + e.toString(), e);
        }
        return conn;
    }

    public void closeConnection() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

}
