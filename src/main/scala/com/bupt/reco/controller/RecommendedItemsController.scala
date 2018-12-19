package com.bupt.reco.controller

import java.util._

import com.bupt.reco.domain.RecommendedItems
import com.bupt.reco.utils.RedisClient
import org.springframework.web.bind.annotation.{RequestMapping, RequestMethod, RequestParam, RestController}
import org.training.spark.proto.Spark.{ItemList, ItemSimilarities, ItemSimilarity}

//实现java集合到scala集合类型的自动转换
import scala.collection.JavaConversions._

@RestController
@RequestMapping(Array("/reco"))
class RecommendedItemsController {

  @RequestMapping(value = Array("/result"),method = Array(RequestMethod.GET))
  def getReco(@RequestParam("userid") userid:String):RecommendedItems = {
    val recommendedItems:RecommendedItems = new RecommendedItems

    val jedis = RedisClient.pool.getResource

    //根据用户的实时行为进行推荐，这里的行为是近乎实时，用户的实时行为打到kafka的topic，
    //实时推荐根据用户的行为进行推荐，推荐结果写redis,推荐请求根据uid进行结果的获取
    //这里的结果是近乎历史的行为

    //获取用户的历史行为数据
    val keyHistory:String = String.format("UI:%s",userid)
    val valueHistory:Array[Byte] = jedis.get(keyHistory.getBytes())
    val userItems = ItemList.parseFrom(valueHistory)
    //获取当前用户所有曾经看过的物品
    val userItemsSet = new TreeSet(userItems.getItemIdsList)


    val key:String = String.format("RUI:%s",userid)
    val value:Array[Byte] = jedis.get(key.getBytes())
    if(value == null || value.length <= 0) {
      return recommendedItems
    }
    //排序实现按照相似度进行：从大往小进行排序
    val recoItemsSet:Set[ItemSimilarity] = new TreeSet[ItemSimilarity](
      new Comparator[ItemSimilarity] {
        @Override
        def compare(item1:ItemSimilarity,item2:ItemSimilarity):Int = {
          if(item1.getSimilarity > item2.getSimilarity) {
            return -1;
          }else if(item1.getSimilarity < item2.getSimilarity){
            return 1;
          }else {
            return 0;
          }
        }
      }
    )
    val result = ItemSimilarities.parseFrom(value)
    recoItemsSet.addAll(result.getItemSimilaritesList)
    //这里进行过滤，取用户没有作用过的10歌
    val recommendedItemsIDs = recoItemsSet.toList
      .filter(item => !userItemsSet.contains(item.getItemId))
      .take(10)     //取前三个元素,takeRight(3),表示从右面开始获取，取出3个元素
      .map(_.getItemId())
    recommendedItems.setItems(recommendedItemsIDs.toArray)
    return recommendedItems
    //下面的策略是根据用户ID的历史行为数据中进行的推荐
    /*
    val key:String = String.format("UI:%s",userid)
    val value:Array[Byte] = jedis.get(key.getBytes())
    if(value == null || value.length <= 0) {
      return recommendedItems
    }

    val userItems = ItemList.parseFrom(value)
    //获取当前用户所有曾经看过的物品
    val userItemsSet = new TreeSet(userItems.getItemIdsList)
    //物品ID加上标志符号，用于后面获取物品的相似度
    val userItemStrs = userItems.getItemIdsList.map("II:" + _).map(str =>  str.getBytes())
    //返回所有key的值，这里的值就是相似的物品及相似度
    //userItemStrs:_*：表示将userItemStrs表示为参数序列
    val similarItems:List[Array[Byte]] = jedis.mget(userItemStrs:_*)

    //排序实现按照相似度进行排序
    val similarItemsSet:Set[ItemSimilarity] = new TreeSet[ItemSimilarity](
      new Comparator[ItemSimilarity] {
        @Override
        def compare(item1:ItemSimilarity,item2:ItemSimilarity):Int = {
          if(item1.getSimilarity > item2.getSimilarity) {
            return 1;
          }else if(item1.getSimilarity < item2.getSimilarity){
            return -1;
          }else {
            return 0;
          }
        }
      }
    )
    //这里的物品代表了用户所有的历史物品
    for(item:Array[Byte] <- similarItems) {
      if(item != null){
        val result = ItemSimilarities.parseFrom(item)
        similarItemsSet.addAll(result.getItemSimilaritesList)
      }
    }
    */
    //这里进行过滤，取用户没有作用过的10歌
//    val recommendedItemsIDs = similarItemsSet.toList
//      .filter(item => !userItemsSet.contains(item.getItemId))
//      .take(10)
//      .map(_.getItemId())
//    recommendedItems.setItems(recommendedItemsIDs.toArray)
//    return recommendedItems
  }
}
