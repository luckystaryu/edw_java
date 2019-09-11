package com.zjpl.edw_java.hbase.Utils;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

public class HBaseOpUtilsTest {
    public static void main(String[] args) {
        List cf = new ArrayList();
        cf.add("info");
        HBaseOpUtils.getInstance().createTableBySplitKeys("ZJPL:CustomerSKUQuota",cf,getSplitKey(),true);
        //HBaseOpUtils.getInstance().saveOrUpdateObject2HBase("ZJPL:CustomerSKUQuota","info",getRowkey(),);
    }
    public static String getRowkey(String tenantId,String officeId,int skuId){
        String rowKey = tenantId + officeId + skuId;
        int splitsCount = 100; //预分区
        //对业务主键hashcode之后取余
        int saltingCode =((skuId+"").hashCode()&Integer.MAX_VALUE)%splitsCount;
        String saltingKey ="" + saltingCode;
        if(saltingCode <10){
            saltingKey = "00" + saltingKey;
        }else if(saltingCode < 100){
            saltingKey = "0" + saltingKey;
        }
        rowKey = saltingKey + rowKey;
        return rowKey;
    }
    public static byte[][] getSplitKey(){
        String[] key_10 = new String[]{
                "1","2","3","4","5","6","7","8","9"
        };
        String[] keys = new String[]{ //100个分区组成splitkeys[][]
                "001","002","003","004","005","006","007","008","009",
                "010","011","012","013","014","015","016","017","018","019",
                "020","021","022","023","024","025","026","027","028","029",
                "030","031","032","033","034","035","036","037","038","039",
                "040","041","042","043","044","045","046","047","048","049",
                "050","051","052","053","054","055","056","057","058","059",
                "060","061","062","063","064","065","066","067","068","069",
                "070","071","072","073","074","075","076","077","078","079",
                "080","081","082","083","084","085","086","087","088","089",
                "090","091","092","093","094","095","096","097","098","099"
        };
        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for(int i =0;i<keys.length;i++){
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter =rows.iterator();
        int i =0;
        while(rowKeyIter.hasNext()){
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }
}
