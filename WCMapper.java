package com.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//4個泛型中,前兩個是指定mapper輸入數據的類型KEYIN是輸入的key的類型,VALUEIN是輸入的value的類型
//map和reduce的數據輸入輸出都是以key-value對的行形式封裝的
//默認情況下,框架傳遞給我們的mapper的輸入數據中,key是要處李的文本中醫行的起始偏移量,這一行的內容作為value
public class WCMapper extends Mapper<LongWritable,Text,Text,LongWritable> {

	 //mapreduce框架美讀一行數據就調用一次該方法
	@Override
	protected void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
		//具體業務邏輯就寫在這個方法體中,而且我們業務要處裡的數據已經被框架傳遞近來,在方法參數中key-value
		//key是這一行數據的起始偏移量value是這一行的文本內容
		
		//將這一行的內容轉換成string類型
		String line = value.toString();
		
		//對這一行的文本暗特定分隔符切分
		String[] words = StringUtils.split(line,"");
		
		//遍歷這個單詞術組輸出為kv形式k:單詞 v:1
		for(String word:words){
			context.write(new Text(word),new LongWritable(1));
		}
	}

}
