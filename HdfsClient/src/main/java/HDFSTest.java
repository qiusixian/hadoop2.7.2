/**
 * @ClassName HdfsTest
 * @Description TODO
 * @Author qiuhaijun
 * @Date 2019/11/15 20:11
 * @Version 1.0
 **/



import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;


public class HDFSTest {

    private FileSystem fs;
    private Configuration configuration;

    //@Before初始化方法   对于每一个测试方法都要执行一次
    @Before
    public void initHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 1 创建配置信息对象
        configuration = new Configuration();
        //设置副本数(有三种方法：在自定义配置文件中设置，在resource目录下设置，在Configuration对象中设置)
        //configuration.set("dfs.replication", "2");
        /*参数优先级： 4个默认的配置文件    <     4个自定义配置文件   <  当前类路径下的配置文件    <  代码中的conf设置*/

        // 2 获取文件系统（连接到hadfs，读取配置文件对象，当前使用用户）
        fs = FileSystem.get(new URI("hdfs://hadoop102:9000"),configuration, "qiuhaijun");

        // 3 打印文件系统（调用tostring方法）

        System.out.println("==========================="+fs.toString());

    }


    /**
     * 1.上传
     * @throws Exception
     */
    @Test
    public void HDFSUpLoad() throws Exception{


        //上传本地文件：通过"FileSystem.copyFromLocalFile（Path src，Patch dst）"可将本地文件上传
        //到HDFS的制定位置上，其中src和dst均为文件的完整路径。具体事例如下：
        /**windows系统中路径分隔符在path中用双反斜杠。为了避免路径在不同系统中不一致的情况，
         我们通过File类的seperator属性，可以实现动态识别在哪个系统中,使用对应的分割符。使代码更严谨。
         */
        fs.copyFromLocalFile(new Path("D:\\BigData\\test\\input\\inputWord\\hello.txt"),
                new Path("/input/hello.txt"));


        fs.copyFromLocalFile(new Path("D:"+ File.separator + "BigData" + File.separator +
                        "test" +File.separator + "input" + File.separator + "inputkv" + File.separator + "kv.txt"),
                new Path("/incat/kv.txt"));
    }


    /**
     * 下载
     * @throws Exception
     */
    @Test
    public void getFileFromHDFS() throws Exception{


        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件效验
        // 2 下载文件
        fs.copyToLocalFile(false, new Path("hdfs://hadoop104:9000//user/admin/mapreduce/wordcount/input/yang3.jpg"),
                new Path("G:/yang3.jpg"), true);
        fs.close();
    }


    /**
     * 创建目录
     * @throws Exception
     */
    @Test
    public void mkdirAtHDFS() throws Exception{

        //创建目录 通过"FileSystem.mkdirs（Path f）"可在HDFS上创建文件夹，其中f为文件夹的完整路径。具体实现如下：
        fs.mkdirs(new Path("hdfs://hadoop104:9000/admin"));
    }

    /**
     * 创建HDFS文件
     * @throws Exception
     */

    @Test
    public void createFile()throws Exception{
        //通过"FileSystem.create（Path f）"可在HDFS上创建文件，其中f为文件的完整路径。具体实现如下：
        fs.create(new Path("/test"));
    }

    /**
     * 删除文件夹
     * @throws Exception
     */
    @Test
    public void deleteAtHDFS() throws Exception{


        //删除文件夹 ，如果是非空文件夹，参数2是否递归删除，true递归
        fs.delete(new Path("hdfs://hadoop14:9000/admin"), true);
    }

    /**
     * 重命名文件或文件夹
     * @throws Exception
     */
    @Test
    public void renameAtHDFS() throws Exception{

        //重命名文件或文件夹
        fs.rename(new Path("hdfs://hadoop14:9000/user/admin/mapreduce/wordcount/test/wc.input"),
                new Path("hdfs://hadoop14:9000/user/admin/mapreduce/wordcount/test/wc1.input"));
    }

    /**
     * 重命名文件或文件夹
     * @throws Exception
     */
    //通过"FileStatus.getPath（）"可查看指定HDFS中某个目录下所有文件。

    /**
     * 查看文件名称、权限、长度、块信息
     * @throws Exception
     */
    @Test
    public void readListFiles() throws Exception {


        // 思考：为什么返回迭代器，而不是List之类的容器：减少内存损耗
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {

                System.out.println("block-offset:" + bl.getOffset());

                String[] hosts = bl.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }

            System.out.println("--------------李冰冰的分割线--------------");
        }
    }


    /**
     * 判断是文件还是文件夹
     * @throws Exception
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void findAtHDFS() throws Exception, IllegalArgumentException, IOException{

        //获取查询路径下的文件状态信息
        FileStatus[] listStatus = fs.listStatus(new Path("/"));


        //遍历所有文件状态
        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                System.out.println("f--" + status.getPath().getName());
            } else {
                System.out.println("d--" + status.getPath().getName());
            }
        }
    }






    /**
     *  需求: 指定一个目录，递归显示该目下所有的子目录及文件。
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     */


    public void  listDirectoryAndFile(String path , FileSystem fs  ) throws Exception {
        //处理当前目录下的子目录及文件，以根目录为例“\”
        //以数组形式列出指定路径下的所有文件和文件夹，元素类型是FileStatus,。
        FileStatus[] listStatus = fs.listStatus(new Path(path));
        //循环迭代作用：列出当前路径下的所有文件和目录
        for (FileStatus fileStatus : listStatus) {
            //通过判断FileStatus对象是否为文件
            if (fileStatus.isFile()) {
                //是文件，还要判断是否就是在当前目录下
                if (path.equals("/")) {
                    //如果是当前目录下，我们打印文件的全路径：当前目录前的绝对路径（path）+当前路径下的文件名（或者目录名）（fileStatus.getPath().getName()）
                    //路径：对象属性层层包装+字符串拼接。
                    System.out.println("File: " + path + fileStatus.getPath().getName());
                } else {
                    //如果不在当前目录下，那就要列出路径：路径 = /+目录名
                    System.out.println("File: " + path + "/" + fileStatus.getPath().getName());
                }

            } else {
                //不是文件，那么就是目录，
                //System.out.println(fileStatus.getPath());打印出当前目录/文件的绝对路径（包括了协议，进程IP地址，端口号和hdfs系统的文件/目录的物理路径），我们所看到的模样，其实是toSting（）方法中对该对象属性的拼接形式 ：hdfs://hadoop102:9000/bigdata0615
                //System.out.println("Dir: " +fileStatus.getPath().getName());打印出当前目录/文件路径的最后一层目录/文件名bigdata0615

                //我要获取当前路径，就需要对绝对路径进行切割。调用substring（）方法进行切割，只用获取"hdfs://hadoop102:9000"之后的一层路径。该方法定义public String substring(int beginIndex)，调用该方法时传入被切字符串的长度，int beginIndex表示从该长度后开始切。我们这里从字符串"hdfs://hadoop102:9000"之后开切（因为当前目录前都是一样的）"hdfs://hadoop102:9000".length()，表示字符串"hdfs://hadoop102:9000"的长度。

                String currentPath = fileStatus.getPath().toString().substring("hdfs://hadoop102:9000".length());

                System.out.println("Dir: " + currentPath);

                //递归显示当前目录下的子目录及文件：在该方法中调用方法自己，把当前路径作为参数传进去，直到目录为空停止递归调用。
                //递归进去，怎么出来的？
                //FileStatus[] listStatus = fs.listStatus(new Path(path));
                listDirectoryAndFile(currentPath, fs);
            }
        }
    }
}