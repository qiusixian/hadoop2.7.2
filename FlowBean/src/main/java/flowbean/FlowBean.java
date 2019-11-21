package flowbean; /**
 * @ClassName FlowBean
 * @Description TODO
 * @Author qiuhaijun
 * @Date 2019/11/15 20:07
 * @Version 1.0
 **/

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 上行流量
 *
 * 下行流量
 *
 * 总流量
 */
/*定义一个Javabean类实现序列化接口，该类的属性携带最终的数据，通过重写toString()方法来获取属性输出的形式*/
public class FlowBean  implements Writable {

    //1.定义私有属性
    private  Long upFlow ;   //上行流量

    private  Long downFlow ; //下行流量

    private  Long sumFlow ;  //总流量

    //2、定义构造函数：反序列化时，需要反射调用空参构造函数，所以必须有空参构造方法
    public FlowBean() {
    }

    //这个构造函数是在哪里用?
    public FlowBean(long upFlow, long downFlow) {
        super();
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    //3.定义getter和setter方法，用于获取属性和给属性赋值
    public Long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Long upFlow) {
        this.upFlow = upFlow;
    }

    public Long getDownFLow() {
        return downFlow;
    }

    public void setDownFLow(Long downFLow) {
        this.downFlow = downFLow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }
    /*重写toString方法，方便将属性后续打印到文本*/
    @Override
    public String toString() {
        return  upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    /**
     * 写（write）序列化方法，将对象写出到磁盘
     * 依赖关系：Write（）方法依赖DataOutput对象的writexxx()方法
     * 而writexxx()方法又依赖当前对象属性。依赖真是无处不在
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }
    /**
     * 反序列化方法
     *
     * 注意: 序列化的顺序与反序列化的顺序一定要保证一致。
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

}
