package com.zjpl.edw_java.Utils;

import com.zjpl.edw_java.conf.ConfigurationManager;
import com.zjpl.edw_java.constant.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * JDBC辅助组件
 */
public class JDBCHelper {
    static {
        try {
            String driver = ConfigurationManager.getProperties(Constants.JDBC_DRIVER);
            Class.forName(driver);
        }catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    private static JDBCHelper instance =null;

    public static JDBCHelper getInstance(){
        if(instance==null){
            synchronized (JDBCHelper.class){
                if(instance==null){
                    instance= new JDBCHelper();
                }
            }
        }
        return instance;
    }

    /**
     * 数据库连接池
     */
    private LinkedList<Connection> datasource = new LinkedList<Connection>();
    /**
     * 私有化构造方法
     * JDBCHelper 在整个程序运行生命周期中，只会创建一次实例化
     * 在这一次创建实例的过程中，就会调用JDBCHelper()构造方法
     * 此时，就可以在构造方法中，去创建为一个的一个数据库连接池
     */
    private JDBCHelper() {
        int datasourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
        //然后创建指定数据量的数据库连接，并放入数据库连接池中
        for(int i=0;i<datasourceSize;i++){
            String url=ConfigurationManager.getProperties(Constants.JDBC_URL);
            String user=ConfigurationManager.getProperties(Constants.JDBC_USER);
            String password=ConfigurationManager.getProperties(Constants.JDBC_PASSWORD);
            try {
                Connection conn= DriverManager.getConnection(url,user,password);
                datasource.push(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * 第四步，提供获取数据连接的方法
     */
    public synchronized Connection getConnection(){
        while(datasource.size()==0){
            try{
                Thread.sleep(10);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    /**
     * 第五步，开发增删改查的方法
     */
    public int executeUpdate(String sql,Object[] params){
        int rnt = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            rnt = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(conn !=null){
                datasource.push(conn);
            }
        }
        return rnt;
    }

    /**
     * 处理查询语句
     * @param sql
     * @param params
     * @param callback
     * @throws Exception
     */
    public void executeQuery(String sql, Object[] params,QueryCallBack callback) throws Exception {
        Connection conn =null;
        PreparedStatement psmt= null;
        ResultSet rs=null;

        try {
            conn= getConnection();
            psmt=conn.prepareStatement(sql);
            for(int i=0;i<params.length;i++){
                psmt.setObject(i+1,params[i]);
            }
            rs =psmt.executeQuery();
            callback.process(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(conn!=null){
                datasource.push(conn);
            }
        }
    }

    /**
     * 批量提交SQL语句
     * @param sql
     * @param paramList
     * @param callBack
     * @return
     */

    public int[] excuteBathQuery(String sql, List<Object[]> paramList,QueryCallBack callBack){
        int[] rtn =null;
        Connection conn= null;
        PreparedStatement psmt=null;

        try {
            conn=getConnection();
            //第一步：使用Connection对象，取消自动提交
            conn.setAutoCommit(false);
            psmt=conn.prepareStatement(sql);

            //第二部：使用PrepareStatement.addBatch()方法调用
            for(Object[] params:paramList){
                for(int i=0;i<params.length;i++)
                {
                    psmt.setObject(i+1,params[i]);
                }
                psmt.addBatch();
            }
            //第三步，使用PrepareStatement.executeBatch()方法，执行批量SQL语句
            rtn=psmt.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(conn!=null){
                datasource.push(conn);
            }
        }
        return rtn;
    }

    /**
     *静态内部类：查询回调接口
     */
   public static interface QueryCallBack {
        /**
         * 处理查询结果
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;
    }
}
