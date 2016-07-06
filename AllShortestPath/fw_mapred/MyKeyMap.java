
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyKeyMap implements WritableComparable<MyKeyMap>{

	IntWritable indI;
	IntWritable indJ;

	public MyKeyMap(int indI, int indJ){
		this.indI = new IntWritable(indI);
		this.indJ = new IntWritable(indJ);
	}

	public MyKeyMap(){
		this.indI = new IntWritable(0);
		this.indJ = new IntWritable(0);
	}	

	// @Override
	public void write(DataOutput out) throws IOException{
		this.indI.write(out);
		this.indJ.write(out);
	}

	// @Override
	public void readFields(DataInput in) throws IOException{
		this.indI.readFields(in);
		this.indJ.readFields(in);
	}

	@Override
	public int compareTo(MyKeyMap keyMap){
		int resComp = this.indI.compareTo(keyMap.indI);
		 if(resComp != 0) return resComp;
		 return this.indJ.compareTo(keyMap.indJ);
	}

	@Override
	public String toString(){
		int a = indI.get(), b = indJ.get();
		return a + " " + b;
	}

}