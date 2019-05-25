/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.assignment2;

import com.fasterxml.jackson.databind.type.ArrayType;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author Sayed
 */
public class fbgrowthmodel  {
    
    public static void main(String[] args) {
        
    }
 {
         SparkSession session =SparkSession.builder()
                 .appName("fbgrowthmodel").config("spark.sql.warehouse.dir","file:///your Directory//")
                 .master("local[*]")
                 .getOrCreate();
         
        

   Dataset<Row> data = session
                .read().option("header", "true")
           .csv("your data set path");
        data.show();   
       JavaRDD<retail> data1;
        data1 = data.toJavaRDD().map((Row t1) -> {
            // throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            retail r = new retail();
            
            String[] fields = t1.toString().replaceAll("[\\[\\]]", "").split(" ");
            r.setArr(fields);
            return r;
         });
StructType schema = new StructType(new StructField[]{ new StructField(
  "items", new org.apache.spark.sql.types.ArrayType(DataTypes.StringType, true),
        false, Metadata.empty())
         });
       Dataset<Row> itemsDF = session.createDataFrame(data1, retail.class);
       itemsDF.show();
       
FPGrowthModel model = new FPGrowth()
  .setItemsCol("items")
  .setMinSupport(0.5)
  .setMinConfidence(0.6)
  .fit(itemsDF);

// Display frequent itemsets.
model.freqItemsets().show();

// Display generated association rules.
model.associationRules().show();

// transform examines the input items against all the association rules and summarize the
// consequents as prediction
model.transform(itemsDF).show();
    }

   
    
    
}
