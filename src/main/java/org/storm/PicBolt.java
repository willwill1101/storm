package org.storm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.storm.http.util.ByteArrayBuffer;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class PicBolt extends BaseBasicBolt {

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		try {
			ByteArrayBuffer img1 =(ByteArrayBuffer) input.getValueByField("img1");
			ByteArrayBuffer img2 =(ByteArrayBuffer) input.getValueByField("img2");
			ByteArrayBuffer img3 =(ByteArrayBuffer) input.getValueByField("img3");
			String path = "C:\\Users\\liuyanghe\\Desktop\\pic\\"+System.currentTimeMillis()+".jpg";
			OutputStream  output =  new FileOutputStream(new File(path));
			IOUtils.write(img1.buffer(), output);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
