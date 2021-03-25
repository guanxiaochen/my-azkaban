package azkaban.jobtype.http;


import azkaban.utils.Props;
import com.alibaba.fastjson.JSONPath;
import org.apache.log4j.Logger;
import org.junit.Test;

public class HttpJobTest {
    public static final Logger LOG = Logger.getLogger(HttpJobTest.class);

    @Test
    public void testHttp() throws Exception {
        Props sysProps = new Props();
        Props jobProps = new Props();
        jobProps.put("url", "http://localhost/dmp/pubJobReleaseTaskPlay");
        jobProps.put("method", "POST");
        jobProps.put("body", "IDS=202006200213804");
        jobProps.put("headers", "Content-Type: application/x-www-form-urlencoded; charset=utf-8");
        jobProps.put("successEval", "$[?(@.code==1)]");
        jobProps.put("status.url", "http://localhost/dmp/checkJobStatus");
        jobProps.put("status.successEval", "$[?(@.code==1)]");
        jobProps.put("status.interval", "500");
        HttpJob job = new HttpJob("httpTest", sysProps, jobProps, LOG);
        job.run();
    }


    @Test
    public void testJpath() throws Exception {
        String json = "{\n" +
                "  \"firstName\": \"John\",\n" +
                "  \"lastName\" : \"doe\",\n" +
                "  \"age\"      : 26,\n" +
                "  \"address\"  : {\n" +
                "    \"streetAddress\": \"naist street\",\n" +
                "    \"city\"         : \"Nara\",\n" +
                "    \"postalCode\"   : \"630-0192\"\n" +
                "  },\n" +
                "  \"phoneNumbers\": [\n" +
                "    {\n" +
                "      \"type\"  : \"iPhone\",\n" +
                "      \"number\": \"0123-4567-8888\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"type\"  : \"home\",\n" +
                "      \"number\": \"0123-4567-8910\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";

        boolean contains = JSONPath.contains(json, "$[?(@.age>0)].test");
        System.out.println(contains);
    }

}