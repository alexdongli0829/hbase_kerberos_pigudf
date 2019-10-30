package com.pigudf;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
 
public class GetClassification extends EvalFunc<String> {

         @Override
         public String exec(Tuple tuple) throws IOException {               
                   Object object = tuple.get(0);
                   int temperature = (Integer)object;
                   if (temperature >= 30){
                            return "Hot";
                   }
                   else if(temperature >=10){
                            return "Moderate";
                   }
                   else {
                            return "Cool";
                   }                
         }
    	public static void main(String[] args)
    	{
       		 System.out.println("Hello world");
    	}
}
