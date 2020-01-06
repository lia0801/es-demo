package com.bigdata.dao;

import com.bigdata.po.Item;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface ItemDao extends ElasticsearchRepository<Item,Long> {

    public List<Item> findByPriceBetween(double i,double j);
}
