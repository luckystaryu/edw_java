package com.zjpl.edw_java.hbase.Utils;

import com.zjpl.edw_java.Utils.YmlUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.hadoop.hbase.HRegionInfo.getTable;

public class HBaseOpUtils {
    private static Configuration conf;
    private static HBaseOpUtils hBaseOpUtils;
    private static Connection conn;
    private static Properties prop;
    private static String springApplicationXML = "application.yml";

    public void init() {
        //初始化Hbase
        conf = HBaseConfiguration.create();
        prop = new Properties();
        prop.setProperty("hbase.zookeeper.quorum","172.16.1.61:2181,172.16.1.62:2181,172.16.1.63:2181");

        try {
            String hbaseConfigFile = null;
        //    String env = (String) YmlUtil.getValue("spring.profiles.active");
//        if(env.equals("dev")){
//            hbaseConfigFile = DatabaseConfiguration.D
//        }
            String quorum =prop.getProperty("hbase.zookeeper.quorum");
            conf.set("hbase.zookeeper.quorum",quorum);
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private HBaseOpUtils() {
    }

    public static HBaseOpUtils getInstance() {
        if (hBaseOpUtils == null) {
            synchronized (HBaseOpUtils.class) {
                if (hBaseOpUtils == null) {
                    hBaseOpUtils = new HBaseOpUtils();
                    hBaseOpUtils.init();
                }
            }
        }
        return hBaseOpUtils;
    }

    public Connection getConn() {
        if (conn == null || conn.isClosed()) {
            try {
                conn = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }

    /**
     * @return
     */
    public Configuration getConfiguration() {
        return conf;
    }

    /**
     * 创建预分区hbase
     *
     * @param tableName
     * @param columnFamily
     * @param splitKeys
     * @param forceDeleteIfExists
     * @return
     */
    @SuppressWarnings("resource")
    public boolean createTableBySplitKeys(String tableName, List<String> columnFamily, byte[][] splitKeys, boolean forceDeleteIfExists) {
        Admin admin = null;
        TableName tableNameEntity = TableName.valueOf(tableName);
        try {
            admin = getConn().getAdmin();
            if (StringUtils.isBlank(tableName) || columnFamily == null || columnFamily.size() < 0) {

            }
            if (admin.tableExists(tableNameEntity)) {
                if (forceDeleteIfExists) {
                    admin.disableTable(tableNameEntity);
                    admin.deleteTable(tableNameEntity);
                } else {
                    return true;
                }
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : columnFamily) {
                tableDescriptor.addFamily(new HColumnDescriptor(cf).setMaxVersions(1));
                admin.createTable(tableDescriptor, splitKeys);//指定splitKeys
                System.out.println("【HBase】=====Create Table " + tableName
                        + " Success!columnFamily:" + columnFamily.toString() + "======");
            }
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            return false;
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 保存或者更新到HBase
     *
     * @param tableName
     * @param columnFamilyName
     * @param rowKey
     * @param objList
     * @throws IOException
     */
    public void saveOrUpdateObjectList2HBase(String tableName, String columnFamilyName, String rowKey, List<Object> objList) throws IOException {
        conn = getConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        List<Put> putList = new ArrayList<Put>();
        objList.parallelStream().forEach(e -> {
            PropertyDescriptor[] pds = PropertyUtils.getPropertyDescriptors(e.getClass());
            BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(e);
            for (PropertyDescriptor propertyDescriptor : pds) {
                String properName = propertyDescriptor.getName();
                if ("class".equals(properName) || ("ROW".equals(properName))) {
                    continue;
                }
                System.out.println("properName =[" + properName + "]");
                String value = (String) beanWrapper.getPropertyValue(properName);
                System.out.println("value = [" + value + "]");

                if (!StringUtils.isBlank(value)) {
                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(properName), Bytes.toBytes(value));
                    putList.add(put);
                }
            }
            try {
                BufferedMutator mutator = null;
                TableName tName = TableName.valueOf(tableName);
                BufferedMutatorParams params = new BufferedMutatorParams(tName);
                params.writeBufferSize(5 * 1024 * 1024);//可以自己设定阀值5M 达到5M则提交一次
                mutator = conn.getBufferedMutator(params);
                mutator.mutate(putList);//数据量达到5M时会自动提交一次
                mutator.flush();
            } catch (IOException ex) {
                ex.printStackTrace();
            } finally {
                try {
                    table.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        });
    }

    public void saveOrUpdateObject2HBase(String tableName, String columnFamilyName, String rowKey, Object obj) throws IOException {
        conn = getConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        List<Put> putList = new ArrayList<Put>();
        Put put = null;
        PropertyDescriptor[] pds = PropertyUtils.getPropertyDescriptors(obj.getClass());
        BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(obj);
        for (PropertyDescriptor propertyDescriptor : pds) {
            String properName = propertyDescriptor.getName();
            if ("class".equals(properName) || ("ROW".equals(properName))) {
                continue;
            }
            System.out.println("properName =[" + properName + "]");
            String value = (String) beanWrapper.getPropertyValue(properName);
            System.out.println("value = [" + value + "]");
            if (!StringUtils.isBlank(value)) {
                put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes(properName), Bytes.toBytes(value));
                putList.add(put);
            }
        }
        try {
            BufferedMutator mutator = null;
            TableName tName = TableName.valueOf(tableName);
            BufferedMutatorParams params = new BufferedMutatorParams(tName);
            params.writeBufferSize(5 * 1024 * 1024);//可以自己设定阀值5M 达到5M则提交一次
            mutator = conn.getBufferedMutator(params);
            mutator.mutate(putList);//数据量达到5M时会自动提交一次
            mutator.flush();
        } finally {
            try {
                table.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
    public List<String> queryTableBatch(List<String> rowkeyList,String tableName) throws IOException {
        List<Get> getList = new ArrayList<>();
        List dataList = new ArrayList();
        Table table = getConn().getTable(TableName.valueOf(tableName));//获取表
        for(String rowkey:rowkeyList){
            //把rowkey加到get里,再把get装到list中
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }
        Result[] results= table.get(getList);
        for(Result result:results){
            //对返回的结果集进行操作
            for(Cell kv:result.rawCells()){
                String value = Bytes.toString(CellUtil.cloneValue(kv));
                dataList.add(value);
            }
        }
        return dataList;
    }
    public List<Object> queryDataObjByRowKeyKeyList(List<String> rowkeyList,String tableName,Class objClass) throws Exception {
        List<Get> getList = new ArrayList<>();
        List dataList = new ArrayList();
        Table table = getConn().getTable(TableName.valueOf(tableName));//获取表
        for(String rowkey:rowkeyList){
            //把rowkey加到get里,再把get装到list中
            Get get = new Get(Bytes.toBytes(rowkey));
            getList.add(get);
        }
        Result[] results= table.get(getList);
        List<Object> dataResult = new ArrayList();
        for(Result result:results){
            //对返回的结果集进行操作
            Object obj = objClass.newInstance();
            Object objResult =convertHBaseResult2Obj(result,obj);
            dataResult.add(objResult);
        }
        return dataResult;
    }

    public Object convertHBaseResult2Obj(Result result, Object obj) throws Exception {
        String qualifier =null;
        String value =null;
        String rowkey =null;
        for(Cell cell:result.listCells()){
            Bytes.toString(CellUtil.cloneFamily(cell));
            rowkey =Bytes.toString(CellUtil.cloneRow(cell));
            qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            value =Bytes.toString(CellUtil.cloneValue(cell));
            if(!StringUtils.isBlank(value)){
                BeanUtils.setProperty(obj,qualifier,value);
                BeanUtils.setProperty(obj,"rowKey",rowkey);
            }
        }
        return obj;
    }

    /**
     * 插入数据
     * @param tableName
     * @param row
     * @param columnFamily
     * @param column
     * @param data
     * @throws IOException
     */
    public void putData(String tableName,String row,String columnFamily,String column,String data) throws IOException {
         Table table =getConn().getTable(TableName.valueOf(tableName));
         try {
             Put put = new Put(Bytes.toBytes(row));
             put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
             table.put(put);
         }finally {
             table.close();
         }
    }

    ThreadLocal<List<Put>> threadLocal = new ThreadLocal<List<Put>>();

    /**
     * 批量增加记录到HBase表，同一线程要保证对相同表进行添加操作
     * @param tableName
     * @param rowkey
     * @param cf
     * @param column
     * @param value
     */
    public void bulkput(String tableName,String rowkey,String cf,String column,String value) {
        List<Put> list = threadLocal.get();
        if(list ==null){
            list = new ArrayList<Put>();
        }

        try {
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));
            list.add(put);
            if (list.size() >= 500) {
                //超过500条数据，批量提交
                HTable table = null;
                table = (HTable) getConn().getTable(TableName.valueOf(tableName));
                table.put(list);
                list.clear();
            } else {
                threadLocal.set(list);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
    }
    public List<Result> getStartEndRow(final String tableName,final String startKey,final String stopKey,final int num) throws IOException {
        List<Result> list =new ArrayList<>();
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        if(num >0){
            //过滤获取的条件
            Filter filterNum = new PageFilter(num);
            //每页展示条数
            fl.addFilter(filterNum);
        }
        //过滤器的添加
        Scan scan = new Scan();
        scan.setStartRow(startKey.getBytes());
        scan.setStopRow(stopKey.getBytes());
        scan.setFilter(fl);
        //为查询设置过滤器的list
        HTable table =(HTable)getConn().getTable(TableName.valueOf(tableName));
        ResultScanner rscanner = table.getScanner(scan);
        for(Result result:rscanner){
            list.add(result);
        }
        return list;
    }
    public List<Result> getPrefixRow(final String tableName,final String prefixStr,int num) throws IOException {
        List<Result> list = new ArrayList<>();
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //设置正则过滤器
        Filter prefixFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new BinaryPrefixComparator(prefixStr.getBytes()));
        if(num >0){
            //过滤获取的条数
            Filter filterNum = new PageFilter(num); //每页展示条数
            fl.addFilter(filterNum);
        }
        //过滤器的添加
        fl.addFilter(prefixFilter);
        //过滤器的添加
        Scan scan = new Scan();
        scan.setFilter(fl);//为查询设置过滤器的list
        HTable table =(HTable)getConn().getTable(TableName.valueOf(tableName));
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result:resultScanner){
            list.add(result);
        }
        return list;
    }
    public Result getDataByRowKey(String tableName,String rowkey) throws IOException {
        HTable table =(HTable)getConn().getTable(TableName.valueOf(tableName));
        Get get = new Get(rowkey.getBytes());
        Result result = table.get(get);
        return result;
    }
    public void delDataByRowKey(String tableName,String rowkey) throws IOException {
        HTable table =(HTable) conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
        table.close();
    }
    public void createTable(String tableName) throws IOException {
        HBaseAdmin admin =(HBaseAdmin)conn.getAdmin();
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor htd_info =new HColumnDescriptor("info");
        htd.addFamily(htd_info);
        htd.addFamily(new HColumnDescriptor("data"));
        htd_info.setMaxVersions(3);
        admin.createTable(htd);
        admin.close();
    }

    /**
     *
     * @param tableName
     * @return
     */
    public static long rowCount(String tableName){
        long rowCount =0;
        @SuppressWarnings("resource")
        AggregationClient aggregationClient = new AggregationClient(conf);
        Scan scan = new Scan();
        try {
            rowCount =aggregationClient.rowCount(TableName.valueOf(tableName),new LongColumnInterpreter(),scan);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return rowCount;
    }
}
