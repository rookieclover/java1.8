package cn.b2b.index.product.dbmanager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.log4j.Logger;

public class MSSQLConnectionImpl {
    static final Logger LOG = Logger.getLogger("create");
    
    private Connection conn;
    private String connurl;
    private String username;
    private String password;
    public static MSSQLConnectionImpl msSQLInstance;


    public MSSQLConnectionImpl(String connurl, String username, String password) {
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
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            LOG.error("Cannot find mysql Driver:" + e.toString(), e);
        }
        conn = DriverManager.getConnection(connurl, username, password);

        return conn;
    }

    public void closeConnection() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

}
