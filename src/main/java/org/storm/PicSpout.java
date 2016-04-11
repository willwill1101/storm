package org.storm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.storm.http.util.ByteArrayBuffer;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class PicSpout extends BaseRichSpout {

	  private SpoutOutputCollector collector;
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector=collector;
		
	}

	@Override
	public void nextTuple() {
		List<Object> tuple = new ArrayList<Object>();
		//ByteArrayBuffer bytes = 
		InputStream input = null;
		try {
			 input =  new FileInputStream(new File ("C:\\Users\\liuyanghe\\Desktop\\21.jpg"));
			ByteArrayBuffer bytes =null;
			byte[] img1 = IOUtils.toByteArray(input);
			bytes= new ByteArrayBuffer(1);
			bytes.append(img1, 0, img1.length);
			tuple.add(bytes);
			byte[] img2 = IOUtils.toByteArray(input);
			bytes= new ByteArrayBuffer(1);
			bytes.append(img2, 0, img2.length);
			tuple.add(bytes);
			byte[] img3 = IOUtils.toByteArray(input);
			bytes= new ByteArrayBuffer(1);
			bytes.append(img3, 0, img3.length);
			tuple.add(bytes);
			this.collector.emit(tuple);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			if(input!=null){
				try {
					input.close();
				} catch (IOException e) {
				}
			}
		}
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("img1","img2","img3"));
		
	}

}
