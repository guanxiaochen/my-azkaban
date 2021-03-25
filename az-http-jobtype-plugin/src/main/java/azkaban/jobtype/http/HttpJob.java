/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.jobtype.http;

import azkaban.jobExecutor.AbstractJob;
import azkaban.utils.Props;
import azkaban.utils.StringUtils;
import azkaban.utils.UndefinedPropertyException;
import com.alibaba.fastjson.JSONPath;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;


/**
 * A http job.
 */
public class HttpJob extends AbstractJob {
    public static final String HTTP_GET = "GET";
    public static final String HTTP_POST = "POST";

    public static final String HEADER_ELEMENT_DELIMITER = "\r\n";
    public static final String HEADER_NAME_VALUE_DELIMITER = ":";

    public static final String URL = "url";
    public static final String METHOD = "method";
    public static final String HEADERS = "headers";
    public static final String BODY = "body";
    public static final String TIMEOUT = "timeout";
    public static final String REQUEST_TIMEOUT = "requestTimeout";
    public static final String CONNECTION_TIMEOUT = "connectionTimeout";
    public static final String SOCKET_TIMEOUT = "connectionTimeout";
    public static final String SUCCESS_EVAL = "successEval";
    public static final String FAIL_EVAL = "failEval";
    public static final String STATUS_PREFIX = "status.";
    public static final String STATUS_INTERVAL = "status.interval";
    public static final String STATUS_MAX_RETRIES = "status.max-retries";

    protected Props jobProps;
    protected boolean isCancel = false;


    public HttpJob(String jobId, Props sysProps, Props jobProps, Logger log) {
        this(jobId, sysProps, jobProps, null, log);
    }

    public HttpJob(String jobId, Props sysProps, Props jobProps, Props privateProps, Logger log) {
        super(jobId, log);
        this.jobProps = jobProps;
    }

    @Override
    public void run() throws Exception {
        HttpRequestBase httpRequest = getHttpRequest("");

        final int timeout = jobProps.getInt(TIMEOUT, 3000);
        final int connectionRequestTimeout = jobProps.getInt(REQUEST_TIMEOUT, timeout);
        final int connectionTimeout = jobProps.getInt(CONNECTION_TIMEOUT, timeout);
        final int socketTimeout = jobProps.getInt(SOCKET_TIMEOUT, timeout);

        final RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(connectionRequestTimeout)
                .setConnectTimeout(connectionTimeout)
                .setSocketTimeout(socketTimeout).build();

        final HttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();

        final long startMs = System.currentTimeMillis();

        boolean success = false;
        try {
            HttpResponse httpResponse = httpClient.execute(httpRequest, HttpClientContext.create());
            int statusCode = httpResponse.getStatusLine().getStatusCode();

            HttpEntity entity = httpResponse.getEntity();
            String content = null;
            if (entity != null) {
                content = IOUtils.toString(entity.getContent(), StandardCharsets.UTF_8);
                this.info("HTTP response [" + content + "]");
            } else {
                this.info("HTTP No response");
            }
            if (statusCode / 100 == 4 || statusCode / 100 == 5) {
                throw new RuntimeException("HTTP execute error, status：" + statusCode + ", message: " + httpResponse.getStatusLine().getReasonPhrase());
            }
            String failEval = jobProps.getString(FAIL_EVAL, "");
            String successEval = jobProps.getString(SUCCESS_EVAL, "");
            this.info("HTTP validate successEval:" + successEval + ", failEval:" + failEval);
            success = StringUtils.isEmpty(failEval) || !isContainsEvals(content, failEval.split(","));
            success = success && (StringUtils.isEmpty(successEval) || isContainsEvals(content, successEval.split(",")));
            if (success && jobProps.containsKey(STATUS_PREFIX + URL)) {
                success = checkStatus();
            }
            if (!success) {
                throw new RuntimeException("Job execute failed ");
            }
        } finally {
            info("HTTP " + getId() + " completed "
                    + (success ? "successfully" : "unsuccessfully") + " in "
                    + ((System.currentTimeMillis() - startMs) / 1000) + " seconds.");
        }
    }

    private boolean isContainsEvals(String content, String[] evals) {
        if (evals.length == 1) {
            return isContainsEval(content, evals[0]);
        } else {
            for (String eval : evals) {
                if (!eval.trim().isEmpty()) {
                    if (isContainsEval(content, eval)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private boolean isContainsEval(String content, String eval) {
        Object extract;
        try {
            extract = JSONPath.extract(content, eval);
        } catch (Exception e) {
            this.warn("JSONPath eval error: " + e.getMessage());
            return false;
        }
        if (extract == null) {
            return false;
        }
        if (extract instanceof Collection) {
            return !((Collection<?>) extract).isEmpty();
        }
        if (extract instanceof Map) {
            return !((Map<?, ?>) extract).isEmpty();
        }
        return true;
    }

    private boolean checkStatus() {
        long interval = jobProps.getLong(STATUS_INTERVAL, 1000);
        int maxRetries = jobProps.getInt(STATUS_MAX_RETRIES, 3);
        String failEval = jobProps.getString(STATUS_PREFIX + FAIL_EVAL, "");
        String successEval = jobProps.getString(STATUS_PREFIX + SUCCESS_EVAL);
        if (StringUtils.isEmpty(failEval)) {
            throw new UndefinedPropertyException("Configuration required " + STATUS_PREFIX + SUCCESS_EVAL);
        }

        final int timeout = jobProps.getInt(STATUS_PREFIX + TIMEOUT, 30000);
        final int connectionRequestTimeout = jobProps.getInt(STATUS_PREFIX + REQUEST_TIMEOUT, timeout);
        final int connectionTimeout = jobProps.getInt(STATUS_PREFIX + CONNECTION_TIMEOUT, timeout);
        final int socketTimeout = jobProps.getInt(STATUS_PREFIX + SOCKET_TIMEOUT, timeout);

        final RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(connectionRequestTimeout)
                .setConnectTimeout(connectionTimeout)
                .setSocketTimeout(socketTimeout).build();

        final HttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();

        int allTimes = 0;
        int errorTimes = 0;
        this.info("HTTP check status interval:" + interval + ", successEval:" + successEval + ", failEval:" + failEval);
        HttpRequestBase httpRequest = getHttpRequest(STATUS_PREFIX);
        while (!isCancel) {
            try {
                Thread.sleep(interval);
            } catch (InterruptedException ignored) {
            }
            try {
                HttpResponse httpResponse = httpClient.execute(httpRequest, HttpClientContext.create());
                int statusCode = httpResponse.getStatusLine().getStatusCode();

                HttpEntity entity = httpResponse.getEntity();
                String content = entity == null ? null : IOUtils.toString(entity.getContent(), StandardCharsets.UTF_8);
                if (statusCode / 100 == 4 || statusCode / 100 == 5) {
                    throw new RuntimeException("HTTP job status check error, status：" + statusCode + ", message: " + httpResponse.getStatusLine().getReasonPhrase() + ", response [" + content + "]");
                }
                if (!StringUtils.isEmpty(failEval) && isContainsEvals(content, failEval.split(","))) {
                    this.info("HTTP job status check response [" + content + "]");
                    return false;
                }
                if (isContainsEvals(content, successEval.split(","))) {
                    this.info("HTTP job status check response [" + content + "]");
                    return true;
                }
                errorTimes = 0;
            } catch (Exception e) {
                this.info("HTTP job status check error: " + e.getMessage());
                if (++errorTimes > maxRetries) {
                    return false;
                }
            } finally {
                this.info("HTTP job status checked " + (++allTimes) + " times");
            }
        }
        return false;
    }

    private HttpRequestBase getHttpRequest(String prefix) {
        String url = jobProps.getString(prefix + URL);
        String method = jobProps.getString(prefix + METHOD, "GET");
        String headers = jobProps.getString(prefix + HEADERS, "");

        HttpRequestBase httpRequest;
        if (HTTP_POST.equals(method)) {
            String body = jobProps.getString(prefix + BODY, "");
            // put together an URL
            this.info("HTTP POST url: " + url);
            final HttpPost httpPost = new HttpPost(url);
            if (!body.isEmpty()) {
                info("HTTP body: " + body);
                httpPost.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
            }
            httpRequest = httpPost;
        } else if (HTTP_GET.equals(method)) {
            // GET
            this.info("HTTP GET url: " + url);
            httpRequest = new HttpGet(url);
        } else {
            throw new UndefinedPropertyException("Unsupported request method: " + method + ". Only POST and GET are supported");
        }

        Header[] httpHeaders = parseHttpHeaders(headers);
        if (httpHeaders != null) {
            httpRequest.setHeaders(httpHeaders);
            info("HTTP headers size: " + httpHeaders.length);
        }
        return httpRequest;
    }

    /**
     * Parse headers
     *
     * @return null if headers is null or empty
     */
    public static Header[] parseHttpHeaders(final String headers) {
        if (headers == null || headers.length() == 0) {
            return null;
        }

        final String[] headerArray = headers.split(HEADER_ELEMENT_DELIMITER);
        final List<Header> headerList = new ArrayList<>(headerArray.length);
        for (final String headerPair : headerArray) {
            final int index = headerPair.indexOf(HEADER_NAME_VALUE_DELIMITER);
            if (index != -1) {
                headerList.add(new BasicHeader(headerPair.substring(0, index),
                        headerPair.substring(index + 1)));
            }
        }
        return headerList.toArray(new Header[0]);
    }

    @Override
    public void cancel() throws Exception {
        isCancel = true;
        super.cancel();
    }

    @Override
    public boolean isCanceled() {
        return isCancel;
    }


}