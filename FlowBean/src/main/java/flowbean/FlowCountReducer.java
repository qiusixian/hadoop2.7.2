package flowbean; /**
 * @ClassName FlowCountReducer
 * @Description TODO
 * @Author qiuhaijun
 * @Date 2019/11/15 20:26
 * @Version 1.0
 **/

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer  extends Reducer<Text, FlowBean, Text, FlowBean>{

    FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {

        // 上行流量
        long totalUpFlow = 0 ;
        long totalDownFlow = 0 ;
        long totalSumFlow = 0 ;

        for (FlowBean flowBean : values) {
            totalUpFlow += flowBean.getUpFlow();
            totalDownFlow += flowBean.getDownFLow();
            totalSumFlow += flowBean.getSumFlow();
        }

        //value
        v.setUpFlow(totalUpFlow);
        v.setDownFLow(totalDownFlow);
        v.setSumFlow(totalSumFlow);

        //写出
        context.write(key, v);
    }
}
