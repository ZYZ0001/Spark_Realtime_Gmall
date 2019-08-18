package com.atguigu.gmall.publisher.service.impl;

import com.atguigu.gmall.publisher.mapper.DauMapper;
import com.atguigu.gmall.publisher.mapper.OrderMapper;
import com.atguigu.gmall.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }

    // 本次未实现获取新增设备的功能
    @Override
    public Long getNewMidTotal(String date) {
        return 100L;
    }

    @Override
    public Map<String, Long> getHourDauCount(String date) {
        List<Map> dauHourCount = dauMapper.getDauHourCount(date);

        HashMap<String, Long> hourMap = new HashMap<>();

        for (Map map : dauHourCount) {
            hourMap.put(map.get("LOGHOUR").toString(), Long.parseLong(map.get("CT").toString()));
        }

        return hourMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.getOrderAmount(date);
    }

    @Override
    public Map<String, Double> getHourOrderAmount(String date) {
        List<Map> orderHourSum = orderMapper.getOrderHourAmount(date);
        HashMap<String, Double> hourMap = new HashMap<>();

        for (Map map : orderHourSum) {
            hourMap.put(map.get("CREATE_HOUR").toString(), Double.parseDouble(map.get("SUM_AMOUNT").toString()));
        }
        return hourMap;
    }

    @Override
    public Map<String, Object> getSaleDetail(String date, int startpage, int size, String keyword) {
        String dt = "dt"; //过滤条件
        String skuName = "sku_name"; //查询条件
        String groupByAge = "groupBy_age"; //分组名称
        String userAge = "user_age"; //分组条件
        String groupByGender = "groupBy_gender"; //分组名称
        String userGender = "user_gender.keyword"; //分组条件

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 构建过滤和匹配条件
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder(dt, date));
        boolQueryBuilder.must(new MatchQueryBuilder(skuName, keyword).operator(MatchQueryBuilder.Operator.AND));
        searchSourceBuilder.query(boolQueryBuilder);

        // 构建分组
        TermsBuilder aggsGroupByAge = AggregationBuilders.terms(groupByAge).field(userAge).size(100);
        TermsBuilder aggsGroupByGender = AggregationBuilders.terms(groupByGender).field(userGender).size(2);
        searchSourceBuilder.aggregation(aggsGroupByAge);
        searchSourceBuilder.aggregation(aggsGroupByGender);

        // 构建分页
        searchSourceBuilder.from((startpage - 1) * size);
        searchSourceBuilder.size(size);

        Search search = new Search.Builder(searchSourceBuilder.toString()).build();
        Map<String, Object> resultMap = new HashMap<>();
        // 构建输出结果集
        try {
            SearchResult result = jestClient.execute(search);

            //total -- 总数
            Long total = result.getTotal();
            resultMap.put("total", total);

            //stat -- 统计 -- 饼图
            MetricAggregation aggregations = result.getAggregations();
            //年龄分组个数
            HashMap<Object, Object> ageMap = new HashMap<>();
            //取出各年龄数据
            List<TermsAggregation.Entry> ageBuckets = aggregations.getTermsAggregation(groupByAge).getBuckets();
            for (TermsAggregation.Entry entry : ageBuckets) {
                ageMap.put(entry.getKey(), entry.getCount());
            }
            resultMap.put("ageMap", ageMap);
            //性别分组个数
            HashMap<Object, Object> genderMap = new HashMap<>();
            List<TermsAggregation.Entry> gerderBuckets = aggregations.getTermsAggregation(groupByGender).getBuckets();
            for (TermsAggregation.Entry entry : gerderBuckets) {
                genderMap.put(entry.getKey(), entry.getCount());
            }
            resultMap.put("genderMap", genderMap);

            //detail -- 明细
            List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
            ArrayList<Map> detailList = new ArrayList<>();
            for (SearchResult.Hit<Map, Void> hit : hits) {
                detailList.add(hit.source);
            }
            resultMap.put("detail", detailList);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return resultMap;
    }
}
