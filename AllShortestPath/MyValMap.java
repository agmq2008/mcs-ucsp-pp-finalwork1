
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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MyValMap implements Writable{

	IntWritable kIter;
	IntWritable indI;
	IntWritable indJ;
	IntWritable distIJ;
	IntWritable prefIJ;

	public MyValMap(int indI, int indJ, int kIter, int distIJ, int prefIJ){
		this.indI = new IntWritable(indI);
		this.indJ = new IntWritable(indJ);
		this.kIter = new IntWritable(kIter);
		this.distIJ = new IntWritable(distIJ);
		this.prefIJ = new IntWritable(prefIJ);
	}

	public MyValMap(){
		this.indI = new IntWritable(0);
		this.indJ = new IntWritable(0);
		this.kIter = new IntWritable(0);
		this.distIJ = new IntWritable(0);
		this.prefIJ = new IntWritable(0);
	}	

	// @Override
	public void write(DataOutput out) throws IOException{
		this.indI.write(out);
		this.indJ.write(out);
		this.kIter.write(out);
		this.distIJ.write(out);
		this.prefIJ.write(out);
	}

	// @Override
	public void readFields(DataInput in) throws IOException{
		this.indI.readFields(in);
		this.indJ.readFields(in);
		this.kIter.readFields(in);
		this.distIJ.readFields(in);
		this.prefIJ.readFields(in);
	}


	@Override
	public String toString(){
		int k = kIter.get(), b = distIJ.get(), c = prefIJ.get();
		int i = indI.get(), j = indJ.get();
		return i + " " + j + " " + k + " " + b + " " + c ;
	}

}