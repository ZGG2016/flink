package com.atguigu.flink.source.bean;

import com.atguigu.flink.source.config.AppConfig;
import com.atguigu.flink.source.enums.DisplayType;
import com.atguigu.flink.source.enums.ItemType;
import com.atguigu.flink.source.enums.PageId;
import com.atguigu.flink.source.util.RandomNum;
import com.atguigu.flink.source.util.RandomNumString;
import com.atguigu.flink.source.util.RandomOptionGroup;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AppPage {


     PageId last_page_id;

     PageId page_id;

     ItemType item_type;

     String item;

     Integer during_time;

     String extend1;

     String extend2;

     DisplayType source_type;




     public static  AppPage build(PageId pageId,PageId lastPageId,Integer duringTime){

          ItemType itemType=null;
          String item=null;
          String extend1=null;
          String extend2=null;
          DisplayType sourceType=null;


          RandomOptionGroup<DisplayType> sourceTypeGroup =   RandomOptionGroup.<DisplayType>builder().add(DisplayType.query, AppConfig.sourceTypeRate[0])
                  .add(DisplayType.promotion, AppConfig.sourceTypeRate[1])
                  .add(DisplayType.recommend, AppConfig.sourceTypeRate[2])
                  .add(DisplayType.activity, AppConfig.sourceTypeRate[3]).build();


          if(pageId== PageId.good_detail  ||pageId== PageId.good_spec||pageId== PageId.comment||pageId== PageId.comment_list){

               sourceType =  sourceTypeGroup.getValue();

               itemType=ItemType.sku_id;
               item= RandomNum.getRandInt(1, AppConfig.max_sku_id)+"";
          }else if(pageId== PageId.good_list){
               itemType=ItemType.keyword;
               item=  new RandomOptionGroup(AppConfig.searchKeywords).getRandStringValue();
          }else if(pageId== PageId.trade||pageId== PageId.payment||pageId== PageId.payment_done) {
               itemType=ItemType.sku_ids;
               item= RandomNumString.getRandNumString(1, AppConfig.max_sku_id,RandomNum.getRandInt(1,3),",",false);
          }
          return   new AppPage(lastPageId, pageId,itemType,item, duringTime,extend1,extend2,sourceType);

     }






}
