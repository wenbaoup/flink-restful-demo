package com.wenbao.flink.restful.flink;

import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class FlinkRestFulUtil {


    /**
     * demo:
     * public static void getAllJobMessage() throws Exception {
     * HttpClient httpClient = HttpClients.createDefault();
     * String url = FlinkWebUrlUtil.getProxyFlinkUrl("flink-stream");
     * HttpGet httpGet = new HttpGet(url + "jobs/overview");
     * System.out.println(url + "jobs/overview");
     * HttpResponse execute = httpClient.execute(httpGet);
     * HttpEntity entity = execute.getEntity();
     * System.out.println(entity);
     * String result = new BufferedReader(new InputStreamReader(entity.getContent()))
     * .lines().collect(Collectors.joining("\n"));
     * System.out.println(result);
     * }
     * <p>
     * 返回所有job的信息
     *
     * @throws Exception
     */
    public static void getAllJobMessage(String applicationName) throws Exception {
        HttpClient httpClient = HttpClients.createDefault();
        String url = FlinkWebUrlUtil.getProxyFlinkUrl(applicationName);
        HttpGet httpGet = new HttpGet(url + "jobs/overview");
        System.out.println(url + "jobs/overview");
        HttpResponse execute = httpClient.execute(httpGet);
        HttpEntity entity = execute.getEntity();
        System.out.println(entity);
        String result = new BufferedReader(new InputStreamReader(entity.getContent()))
                .lines().collect(Collectors.joining("\n"));
        System.out.println(result);
    }

    /**
     * demo:
     * public void getJobMessage() throws Exception {
     * HttpClient httpClient = HttpClients.createDefault();
     * String url = FlinkWebUrlUtil.getProxyFlinkUrl("flink-stream ");
     * HttpGet httpGet = new HttpGet(url + "jobs/0f859a41e25060975719ca7ca0cfb1a9");
     * System.out.println(url + "jobs/0f859a41e25060975719ca7ca0cfb1a9");
     * HttpResponse execute = httpClient.execute(httpGet);
     * HttpEntity entity = execute.getEntity();
     * System.out.println(entity);
     * String result = new BufferedReader(new InputStreamReader(entity.getContent()))
     * .lines().collect(Collectors.joining("\n"));
     * System.out.println(result);
     * }
     * <p>
     * 返回单个job的信息
     *
     * @throws Exception
     */
    public static void getJobMessage(String applicationName, String jobId) throws Exception {
        HttpClient httpClient = HttpClients.createDefault();
        String url = FlinkWebUrlUtil.getProxyFlinkUrl(applicationName);
        HttpGet httpGet = new HttpGet(url + "jobs/" + jobId);
        System.out.println(url + "jobs/" + jobId);
        HttpResponse execute = httpClient.execute(httpGet);
        HttpEntity entity = execute.getEntity();
        System.out.println(entity);
        String result = new BufferedReader(new InputStreamReader(entity.getContent()))
                .lines().collect(Collectors.joining("\n"));
        System.out.println(result);
    }

    /**
     * demo:
     * public void cancelJob() throws Exception {
     * HttpClient httpClient = HttpClients.createDefault();
     * String url = FlinkWebUrlUtil.getProxyFlinkUrl("flink-stream ");
     * HttpGet httpGet = new HttpGet(url + "jobs/0f859a41e25060975719ca7ca0cfb1a9/yarn-cancel");
     * System.out.println(url + "jobs/0f859a41e25060975719ca7ca0cfb1a9/yarn-cancel");
     * HttpResponse execute = httpClient.execute(httpGet);
     * HttpEntity entity = execute.getEntity();
     * System.out.println(entity);
     * String result = new BufferedReader(new InputStreamReader(entity.getContent()))
     * .lines().collect(Collectors.joining("\n"));
     * System.out.println(result);
     * }
     * <p>
     * 取消单个job
     *
     * @throws Exception
     */
    public static void cancelJob(String applicationName, String jobId) throws Exception {
        HttpClient httpClient = HttpClients.createDefault();
        String url = FlinkWebUrlUtil.getProxyFlinkUrl(applicationName);
        HttpGet httpGet = new HttpGet(url + "jobs/" + jobId + "/yarn-cancel");
        System.out.println(url + "jobs/" + jobId + "/yarn-cancel");
        HttpResponse execute = httpClient.execute(httpGet);
        HttpEntity entity = execute.getEntity();
        System.out.println(entity);
        String result = new BufferedReader(new InputStreamReader(entity.getContent()))
                .lines().collect(Collectors.joining("\n"));
        System.out.println(result);
    }

    /**
     * demo:
     * public static String getJarsMessage() throws Exception {
     * HttpClient httpClient = HttpClients.createDefault();
     * String url = FlinkWebUrlUtil.getProxyFlinkUrl("flink-stream ");
     * HttpGet httpGet = new HttpGet(url + "jars");
     * System.out.println(url + "jars");
     * HttpResponse execute = httpClient.execute(httpGet);
     * HttpEntity entity = execute.getEntity();
     * System.out.println(entity);
     * String result = new BufferedReader(new InputStreamReader(entity.getContent()))
     * .lines().collect(Collectors.joining("\n"));
     * System.out.println(result);
     * return result;
     * }
     * <p>
     * <p>
     * 获取所有已经上传jar包的信息
     *
     * @throws Exception
     */

    public static void getJarsMessage(String applicationName) throws Exception {
        HttpClient httpClient = HttpClients.createDefault();
        String url = FlinkWebUrlUtil.getProxyFlinkUrl(applicationName);
        HttpGet httpGet = new HttpGet(url + "jars");
        System.out.println(url + "jars");
        HttpResponse execute = httpClient.execute(httpGet);
        HttpEntity entity = execute.getEntity();
        System.out.println(entity);
        String result = new BufferedReader(new InputStreamReader(entity.getContent()))
                .lines().collect(Collectors.joining("\n"));
        System.out.println(result);
    }

    /**
     * demo:
     * public static void flinkJarUpload() throws Exception {
     * CloseableHttpClient httpClient = HttpClients.createDefault();
     * String url = FlinkWebUrlUtil.getRealFlinkUrl("flink-stream ");
     * HttpPost uploadFile = new HttpPost(url + "/jars/upload");
     * System.out.println(url + "/jars/upload");
     * MultipartEntityBuilder builder = MultipartEntityBuilder.create();
     * builder.addBinaryBody(
     * "jarfile",
     * new FileInputStream("C:\\Users\\10104\\Desktop\\flink-bigscreen-stat-1.0.0-jar-with-dependencies.jar"),
     * ContentType.create("application/x-java-archive"),
     * "flink-bigscreen-stat-1.0.0-jar-with-dependencies.jar"
     * );
     * HttpEntity multipart = builder.build();
     * uploadFile.setEntity(multipart);
     * System.out.println(uploadFile.getURI().toString());
     * CloseableHttpResponse response = httpClient.execute(uploadFile);
     * String result = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))
     * .lines().collect(Collectors.joining("\n"));
     * System.out.println(result);
     * }
     * <p>
     * 上传jar包
     *
     * @throws Exception
     */
    public static void flinkJarUpload(String applicationName, String jarAddress, String jarName) throws Exception {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        String url = FlinkWebUrlUtil.getRealFlinkUrl(applicationName);
        HttpPost uploadFile = new HttpPost(url + "/jars/upload");
        System.out.println(url + "/jars/upload");
        MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.addBinaryBody(
                "jarfile",
                new FileInputStream(jarAddress),
                ContentType.create("application/x-java-archive"),
                jarName
        );
        HttpEntity multipart = builder.build();
        uploadFile.setEntity(multipart);
        System.out.println(uploadFile.getURI().toString());
        CloseableHttpResponse response = httpClient.execute(uploadFile);
        String result = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))
                .lines().collect(Collectors.joining("\n"));
        System.out.println(result);
    }

    /**
     * demo:
     * public static void flinkJarRun() throws Exception {
     * String flinkWebUrl = FlinkWebUrlUtil.getRealFlinkUrl("flink-stream ");
     * System.out.println(flinkWebUrl);
     * HttpClient httpClient = HttpClients.createDefault();
     * HttpPost httpPost = new HttpPost(flinkWebUrl + "/jars/4438ca5a-cd48-49dc-9a65-88db7734757d_flink-bigscreen-stat-1.0.0-jar-with-dependencies.jar/run");
     * System.out.println(flinkWebUrl + "/jars/4438ca5a-cd48-49dc-9a65-88db7734757d_flink-bigscreen-stat-1.0.0-jar-with-dependencies.jar/run");
     * JSONObject jsonObj = new JSONObject();
     * jsonObj.put("entryClass", "com.yjp.stream.stat.client.StatClient");
     * String[] strings = new String[1];
     * strings[0] = "latest_order_stat";
     * jsonObj.put("programArgsList", strings);
     * System.out.println(jsonObj.toString());
     * StringEntity entity = new StringEntity(jsonObj.toString(), ContentType.APPLICATION_JSON);
     * httpPost.setEntity(entity);
     * HttpResponse httpResponse = httpClient.execute(httpPost);
     * <p>
     * System.out.println(httpResponse.getStatusLine().getStatusCode());
     * HttpEntity response = httpResponse.getEntity();
     * System.out.println(response);
     * String result = new BufferedReader(new InputStreamReader(response.getContent()))
     * .lines().collect(Collectors.joining("\n"));
     * System.out.println(result);
     * }
     * <p>
     * 运行jar包
     *
     * @throws Exception
     */

    public static void flinkJarRun(String applicationName, String jarId, JSONObject jsonObj) throws Exception {
        String flinkWebUrl = FlinkWebUrlUtil.getRealFlinkUrl(applicationName);
        System.out.println(flinkWebUrl);
        HttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(flinkWebUrl + "/jars/" + jarId + "/run");
        System.out.println(flinkWebUrl + "/jars/" + jarId + "/run");
        StringEntity entity = new StringEntity(jsonObj.toString(), ContentType.APPLICATION_JSON);
        httpPost.setEntity(entity);
        HttpResponse httpResponse = httpClient.execute(httpPost);
        System.out.println(httpResponse.getStatusLine().getStatusCode());
        HttpEntity response = httpResponse.getEntity();
        System.out.println(response);
        String result = new BufferedReader(new InputStreamReader(response.getContent()))
                .lines().collect(Collectors.joining("\n"));
        System.out.println(result);
    }


    public static void main(String[] args) throws Exception {
        getAllJobMessage("flink-stream");
        getJobMessage("flink-stream", "b59ac2a0d3630d08952557df871bf8e8");
        cancelJob("flink-stream", "b59ac2a0d3630d08952557df871bf8e8");
        getJarsMessage("flink-stream");
        flinkJarUpload("flink-stream"
                , "C:\\Users\\10104\\Desktop\\flink-bigscreen-stat-1.0.0-jar-with-dependencies.jar"
                , "flink-bigscreen-stat-1.0.0-jar-with-dependencies.jar");

        JSONObject jsonObj = new JSONObject();
        jsonObj.put("entryClass", "com.yjp.stream.stat.client.StatClient");
        String[] strings = new String[1];
        strings[0] = "latest_order_stat";
        jsonObj.put("programArgsList", strings);
        flinkJarRun("flink-stream"
                , "4438ca5a-cd48-49dc-9a65-88db7734757d_flink-bigscreen-stat-1.0.0-jar-with-dependencies.jar"
                , jsonObj);
//        flinkJarUpload();

//        flinkJarRun();
//        getAllJobMessage();
    }

}
