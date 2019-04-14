package cn.it.hdfs.demo1;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class HdfsOperateStudy {


    /**
     * 通过url注册的方式来访问hdfs  了解，不会用到
     */
    @Test
    public void getHdfsFile() throws Exception {
        // System.out.println("hello world");
        //注册url驱动
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        //使用输入流去读取文件
        //使用输出流，将文件写到哪里去
        //通过new url的方式打开一个文件流
        InputStream inputStream = new URL("hdfs://node01:8020/test/input/install.log").openStream();
        FileOutputStream outputStream = new FileOutputStream(new File("f:\\hello.txt"));
        //通过工具类将文件流写出去
        IOUtils.copy(inputStream, outputStream);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);

    }


    /**
     * 通过fileSystem获取分布式文件系统的几种方式
     */
    @Test
    public void getFileSystem1() throws IOException {
        //如果configuration 不做任何配置，获取到的是本地文件系统
        Configuration configuration = new Configuration();
        //覆盖hdfs的配置，得到分布式文件系统
        configuration.set("fs.defaultFS", "hdfs://node01:8020/");
        FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println(fileSystem.toString());
    }

    /**
     * 获取hdfs的第二种方式
     */
    @Test
    public void getHdfs2() throws URISyntaxException, IOException {
        //使用两个参数来获取hdfs文件系统
        //第一个参数是一个URI，定义了我们使用hdfs://这种方式来访问，就是分布式文件系统
        //
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        System.out.println(fileSystem.toString());
    }


    /**
     * 获取hdfs分布式文件系统的第三种方式
     */
    @Test
    public void getHdfs3() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        System.out.println(fileSystem.toString());
    }

    /**
     * 获取hdfs分布式文件系统的第四种方式  通过newInstance传递两个参数
     */
    @Test
    public void getFileSystem4() throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://node01:8020"), configuration);
        System.out.println(fileSystem.toString());
    }


    /*
    递归遍历hdfs当中所有的文件路径
     */
    @Test
    public void getAllHdfsFilePath() throws Exception {

        //获取分布式文件系统的客户端
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());

        //给定我们hdfs的根路径
        Path path = new Path("/");
        //通过调用listStatus获取到所有根路径下面的文件的状态
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        //循环遍历fileStatuses  如果是文件，打印文件的路径，如果是文件夹，继续递归进去
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()) {
                getDirectoryFile(fileSystem, fileStatus);

            } else {
                //这里的path其实就是hdfs上面的路径
                Path path1 = fileStatus.getPath();
                System.out.println(path1.toString());
            }
        }
        //关闭客户端
        fileSystem.close();
    }

    private void getDirectoryFile(FileSystem fileSystem, FileStatus fileStatus) throws IOException {
        //通过fileStatus获取到文件夹的路径
        Path path = fileStatus.getPath();
        //通过路径继续往里面遍历，获取到所有的文件夹下面的fileStatuses
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        for (FileStatus status : fileStatuses) {
            if (status.isDirectory()) {
                getDirectoryFile(fileSystem, status);
            } else {
                //打印文件的路径
                System.out.println(status.getPath().toString());
            }
        }
    }


    /**
     * 遍历hdfs上面所有的文件
     * fileSystem.listFiles获取迭代器对象
     */
    @Test
    public void listHdfsFiles() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        Path path = new Path("/");
        //alt  +  shift  +  l  提取变量
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(path, true);

        //遍历迭代器，获取迭代器里面每一个元素
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            Path path1 = next.getPath();
            System.out.println(path1.toString());
        }
        fileSystem.close();
    }


    /**
     * 下载hdfs文件到本地
     * fileSystem.open（）获取文件输入流
     */
    @Test
    public void copyHdfsToLocal() throws Exception {
        //获取分布式文件系统的客户端
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());

        //给定hdfs文件的路径
        Path path = new Path("/test/input/install.log");
        //使用一个输入流去读取hdfs的文件
        FSDataInputStream inputStream = fileSystem.open(path);

        //输出流，将数据输出到本地路径下面去
        FileOutputStream outputStream = new FileOutputStream(new File("e:\\myinstall.log"));
        IOUtils.copy(inputStream, outputStream);

        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);

        //通过copyToLocalFile来将hdfs的文件下载到本地
        // fileSystem.copyToLocalFile(new Path("hdfs://node01:8020/test/input/install.log"),new Path("file:///c:\\myinstall2.log"));
        fileSystem.close();
    }


    /**
     * hdfs上面创建文件夹
     */
    @Test
    public void createHdfsDir() throws Exception {
        //获取分布式文件系统的客户端对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        fileSystem.mkdirs(new Path("/abc/bbc/ddd"));
        fileSystem.close();
    }


    /**
     * hdfs的文件上传
     *  fileSystem.copyFromLocalFile()直接完成
     *  或用fileSystem.create()获取输出流
     */
    @Test
    public void uploadFileToHdfs() throws Exception {
        //获取分布式文件系统的客户端
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //通过copyFromLocalFile 将本地文件上传到hdfs上面去
        fileSystem.copyFromLocalFile(false, new Path("file:///c:\\myinstall.log"), new Path("/abc/bbc/ddd"));

       /*
        第二种文件上传方式，通过流的方式来做
       //输出流，负责将数据输出到hdfs的路径上面去
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/abc/bbc/ddd/abc.txt"));

        //通过输入流读取本地文件系统的文件
        FileInputStream fileInputStream = new FileInputStream(new File("c:\\myinstall.log"));
        IOUtils.copy(fileInputStream,fsDataOutputStream);

        IOUtils.closeQuietly(fsDataOutputStream);
        IOUtils.closeQuietly(fileInputStream);*/
        fileSystem.close();
    }


    /**
     * hdfs的权限校验机制
     */
    @Test
    public void hdfsPermission() throws Exception {
        // FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //通过伪造用户来获取分布式文件系统的客户端
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "root");
        //
        fileSystem.copyToLocalFile(new Path("hdfs://node01:8020/config/core-site.xml"), new Path("file:///c:\\core-site.xml"));
        fileSystem.close();
    }


    /**
     * hdfs在上传小文件的时候进行合并
     */
    @Test
    public void mergeFile() throws Exception {
        //在hdf上面创建一个文件
        //获取本地文件，上传到hdfs创建的文件里面去
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "root");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/bigfile.xml"));

        //获取本地所有小文件的输入流
        //首先获取本地文件系统
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("file:///F:\\传智播客大数据离线阶段课程资料\\3、大数据离线第三天\\上传小文件合并"));
        for (FileStatus fileStatus : fileStatuses) {
            Path path = fileStatus.getPath();
            FSDataInputStream inputStream = localFileSystem.open(path);
            IOUtils.copy(inputStream, fsDataOutputStream);
            //通过拷贝，将本地文件上传到hdfs上面去
            IOUtils.closeQuietly(inputStream);
        }
        //关闭输出流
        IOUtils.closeQuietly(fsDataOutputStream);
        //关闭客户端
        fileSystem.close();
        localFileSystem.close();
    }


}
