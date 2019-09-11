package com.zjpl.edw_java.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class kafkaService {
    @Autowired
    static KafkaTemplate kafkaTemplate;
    private static String canalTopicPrefix ="example";
    public static void sendMessage(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChange = null;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            CanalEntry.EventType eventType = rowChange.getEventType();
            String tableName = entry.getHeader().getTableName();
            String schemaName = entry.getHeader().getSchemaName();
            long executeTime = entry.getHeader().getExecuteTime();

            //根据binlog的filename和position来定位
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {

                Map<String, Object> map = new HashMap<>();

                map.put("event_timestamp", executeTime);
                map.put("table_name", tableName);
                map.put("database_name", schemaName);
                Map<String, Object> map_info = new HashMap<>();

                if (eventType == CanalEntry.EventType.DELETE) {
                    map.put("policy_cred", "delete");
                    for(CanalEntry.Column column : rowData.getBeforeColumnsList()){
                        if(column.getValue()!=null&&!column.getValue().equals(""))
                            map_info.put(column.getName(), column.getValue());
                    }
                } else if(eventType == CanalEntry.EventType.INSERT){
                    map.put("policy_cred", "insert");
                    for(CanalEntry.Column column : rowData.getAfterColumnsList()){
                        map_info.put(column.getName(), column.getValue());
                    }
                }else {
                    map.put("policy_cred", "update");
                    for(CanalEntry.Column column : rowData.getAfterColumnsList()){
                        map_info.put(column.getName(), column.getValue());
                    }

                    Map<String, Object> beforeMap = new HashMap<>();

                    for(CanalEntry.Column column : rowData.getBeforeColumnsList()){
                        if(column.getValue()!=null&&!column.getValue().equals(""))
                            beforeMap.put(column.getName(), column.getValue());
                    }
                    map.put("beforeColumns", beforeMap);
                }
                map.put("map_info",map_info);
                System.out.println(map);
                kafkaTemplate.send( canalTopicPrefix + tableName, JSON.toJSONString(map));

            }
        }
    }
}
