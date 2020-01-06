package com.leyou;

import com.bigdata.EsApplication;
import com.bigdata.dao.ItemDao;
import com.bigdata.po.Item;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = EsApplication.class)
public class ETest {
    @Autowired
    private ElasticsearchTemplate template;
    @Autowired
    private ItemDao itemDao;
    @Test
    public void init(){
        template.createIndex(Item.class);//建库
        template.putMapping(Item.class);//建表
       // template.deleteIndex(Item.class);//删除库
    }

    //添加一个或在已有的基础上修改
    @Test
    public void index() {
        Item item = new Item(1L, "大米手机7", " 手机",
                "小米", 3499.00, "http://image.leyou.com/a.png");
        itemDao.save(item);
    }

    //批量插入
    @Test
    public void indexList() {
        List<Item> list = new ArrayList<>();
        //list.add(new Item(1L, "小米手机7", "手机", "小米", 3299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(2L, "坚果手机R1", "手机", "锤子", 3699.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(3L, "华为META10", "手机", "华为", 4499.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(4L, "小米Mix2S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(5L, "荣耀V10", "手机", "华为", 2799.00, "http://image.leyou.com/13123.jpg"));
        // 接收对象集合，实现批量新增
        itemDao.saveAll(list);
    }

    //删除
    @Test
    public void deleteItem(){
        itemDao.deleteById(2L);
    }

    //查询
    @Test
    public  void  testFind(){
        Iterable<Item> all=itemDao.findAll();
        for (Item item:all){
            System.out.println("item="+item);
        }
    }
    //排序
    @Test
    public void query(){
        // 查询全部，并按照价格降序排序
        Iterable<Item> items = itemDao.findAll(Sort.by("price").descending());//降序
        for (Item item : items) {
            System.out.println("item = " + item);
        }
    }
    //自定义
    @Test
    public  void testFindBy(){
        List<Item> items=itemDao.findByPriceBetween(2000d,4000d);
        for (Item item : items) {
            System.out.println("item = " + item);

        }
    }

    //条件查询
    @Test
    public void search(){
        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 添加基本分词查询
        queryBuilder.withQuery(QueryBuilders.matchQuery("title", "小米手机"));
        // 搜索，获取结果
        Page<Item> items = itemDao.search(queryBuilder.build());
        // 总条数
        long total = items.getTotalElements();
        System.out.println("total = " + total);
        for (Item item : items) {
            System.out.println(item);
        }
    }

    //分页查询
    @Test
    public void searchByPage(){
        // 构建查询条件
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
        // 添加基本分词查询
        queryBuilder.withQuery(QueryBuilders.termQuery("category", "手机"));
        // 分页：从0开始
        queryBuilder.withPageable(PageRequest.of(0,2));

        // 搜索，获取结果
        Page<Item> items = itemDao.search(queryBuilder.build());

        // 总条数
        System.out.println("总条数 = " + items.getTotalElements());

        // 总页数
        System.out.println("总页数 = " + items.getTotalPages());

        // 当前页
        System.out.println("当前页：" + items.getNumber());

        // 每页大小
        System.out.println("每页大小：" + items.getSize());

        for (Item item : items) {
            System.out.println(item);
        }
    }

    //排序(按照价格)
    @Test
    public void orderItem(){
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        nativeSearchQueryBuilder.withQuery(QueryBuilders.termQuery("category","手机"));
        nativeSearchQueryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC));
        Page<Item> search = itemDao.search(nativeSearchQueryBuilder.build());
        List<Item> content = search.getContent();
        for (Item i:content
             ) {
            System.out.println(i);
        }
    }

    //-------聚合-(分组)----------------------------
    @Test
    public void groupItem(){
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();

        //不查询结果
        nativeSearchQueryBuilder.withSourceFilter(new FetchSourceFilter(new String[]{},null));

        //添加聚合  name=tom是聚合名称
        nativeSearchQueryBuilder.addAggregation(AggregationBuilders.terms("tom").field("brand"));

        //查询
        AggregatedPage<Item> items=(AggregatedPage<Item>) itemDao.search(nativeSearchQueryBuilder.build());
        StringTerms tom = (StringTerms) items.getAggregation("tom");
        List<StringTerms.Bucket> buckets = tom.getBuckets();
        for (StringTerms.Bucket s:buckets
             ) {
            System.out.println(s.getKey()+"------"+s.getDocCount());
        }
    }
}

