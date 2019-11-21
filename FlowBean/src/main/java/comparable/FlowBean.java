package comparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * 用于封装一个手机号对应的上行流量  下行流量  总流量
 *hadoop的默认排序是按字典顺序进行快排。
 *
 * 自定义类作为map输出的key的类型，必须实现比较接口，因为map输出后在suffer阶段要对key进行排序。能实现
 * 排序，则必须能够做比较。
 * 还有自定义的类必须实现序列化接口，因为会对map输出的key和value进行落盘。
 * WritableComparable既能排序又能比较。
 *
 */
public class FlowBean  implements WritableComparable<FlowBean>{
	
	private Long upFlow ;
	private Long downFLow ;
	private Long sumFlow ;
	
	public FlowBean() {
	}
	
	
	
	public FlowBean(Long upFlow, Long downFLow) {
		super();
		this.upFlow = upFlow;
		this.downFLow = downFLow;
		this.sumFlow = upFlow + downFLow;
	}



	public Long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(Long upFlow) {
		this.upFlow = upFlow;
	}
	public Long getDownFLow() {
		return downFLow;
	}
	public void setDownFLow(Long downFLow) {
		this.downFLow = downFLow;
	}
	public Long getSumFlow() {
		return sumFlow;
	}
	public void setSumFlow(Long sumFlow) {
		this.sumFlow = sumFlow;
	}
	
	/**
	 * 序列化方法
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFLow);
		out.writeLong(sumFlow);
	}
	
	/**
	 * 反序列化方法
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		downFLow = in.readLong();
		sumFlow = in.readLong();
	}

	@Override
	public String toString() {
		return  upFlow+"\t"+downFLow+"\t"+sumFlow;
	}


	/**
	 * 比较方法
	 * 
	 * 排序规则: 按照总流量降序排序，总流量作为key。其他部分作为value。
	 * compareTo比较的，一般是对象的属性。
	 *
	 * 返回的是一个int值，int值大于0，则表明传入的对象属性小于当前对象属性。我们可以随便设置一个大于
	 * 0的数，来表示当前对象属性大于传入对象属性
	 *
	 * 同理，用一个小于0的数，来表示当前对象属性小于传入对象属性。
	 * int等于0，则表示俩个对象属性相等。
	 */
	@Override
	public int compareTo(FlowBean o) {
		int result  ;
		if(this.getSumFlow() > o.getSumFlow()) {
			result  = -1 ;
		}else if(this.getSumFlow() < o.getSumFlow()) {
			result = 1 ;
		}else {
			result = 0 ;
		}
		return result ;
	}
	
}
