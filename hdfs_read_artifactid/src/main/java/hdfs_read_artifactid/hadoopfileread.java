package hdfs_read_artifactid;

import java.io.FileNotFoundException;
import java.net.URI;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class hadoopfileread {
	public static void main(String[] args) throws Exception {
		String uri = args[0];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try {
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			System.out.println("End Of file: HDFS file read complete");
		} 
		catch (FileNotFoundException e) {
			System.out.println(e);
			System.out.println("Incorrect Input - Please verify Filename");
		} 
		finally {
			IOUtils.closeStream(in);
		}	
	}
}
