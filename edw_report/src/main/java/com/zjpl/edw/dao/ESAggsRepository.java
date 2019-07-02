package com.zjpl.edw.dao;

import com.zjpl.edw.page.BootstrapTablePaginationVo;
import com.zjpl.edw.param.BasicSearchParam;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;


/**
 * ES 的统计 聚合 dao层
 */
@Component
public class ESAggsRepository {
    private static final Logger LOG = LoggerFactory.getLogger(ESAggsRepository.class);
    @Autowired
    private TransportClient client;


    /**
     * 搜索参数
     */
    public BootstrapTablePaginationVo<String> searchMsgByParam(BasicSearchParam param) throws Exception{
        String keyWord = param.getKeyWord();
        String filed = param.getField();
        String index = param.getIndex();

        Assert.assertNotNull(client);
        Assert.assertNotNull(filed);
        Assert.assertNotNull(index);
        Assert.assertNotNull(keyWord);

        //校验索引是否成功
        if(!isIndexExist(index)){//
            return null;
        }
        BootstrapTablePaginationVo<String> vo = new BootstrapTablePaginationVo<String>();

        //响应信息
        List<String> responseStrList = new ArrayList<String>();
        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(filed,keyWord);
        SearchRequestBuilder sbr = client.prepareSearch(index).setQuery(matchQueryBuilder);
        //去重的字段
        if(param.getDistinctField() !=null){
            //去重的信息
            CollapseBuilder cb = new CollapseBuilder(param.getDistinctField());
            sbr.setCollapse(cb);
        }
        CardinalityAggregationBuilder acb = AggregationBuilders.cardinality("count_id").field(param.getDistinctField());
        //列表参数
        SearchResponse response = sbr.addAggregation(acb).setFrom(param.getOffset()).setSize(param.getLimit()).get();
        SearchHits shList = response.getHits();
        for (SearchHit searchHit:shList){
            responseStrList.add(searchHit.getSourceAsString());
        }
        vo.setRows(responseStrList);
        //统计模块
        SingleValue  responseAgg = response.getAggregations().get("count_id");
        int count =0;
        if(responseAgg !=null){
            double value = responseAgg.value();
            count = getInt(value);
        }
        vo.setTotal(count);
        return vo;
    }
    public static int getInt(double number){
        BigDecimal bd = new BigDecimal(number).setScale(0,BigDecimal.ROUND_HALF_UP);
        return Integer.parseInt(bd.toString());
    }

    //不要删除，留着备用
//    public static void getArticleInfoByTags(String tags,List<String> filterList) throws Exception{
//        SearchRequestBuilder
//    }
}
