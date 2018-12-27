package com.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;


public class HdfsUtill {
	FileSystem fs = null;
	@Before
	public void init() throws Exception{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS","hdfs://mlhadoop:9000");
		fs = FileSystem.get(new URI("hdfs://mlhadoop:9000"),conf,"user");
	}
	
	@Test
	public void upload() throws IOException {

		Path src = new Path("hdfs://mlhadoop:9000/test/test1.txt");
		FSDataOutputStream os = fs.create(src);
		FileInputStream in = new FileInputStream("/home/user/test2.txt");
	    IOUtils.copy(in, os);
	}
	
	@Test
	public void download() throws Exception{
		fs.copyToLocalFile(new Path("hdfs://mlhadoop:9000/test/test1.txt"),new Path("/home/user/Downloads/test2.txt"));
	}
}
