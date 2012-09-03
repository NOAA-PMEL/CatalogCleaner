package gov.noaa.pmel.tmap.cleaner.http;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;


import org.apache.commons.httpclient.Cookie;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpState;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.apache.commons.httpclient.params.HttpMethodParams;


public class Proxy {
	private int streamBufferSize = 8196;
	public void executeGetMethodAndSaveResult(String url, File outfile) throws IOException, HttpException {
		HttpClient client = new HttpClient();
		HttpClientParams params = client.getParams();
		params.setParameter(HttpClientParams.SO_TIMEOUT, 400000);
		params.setParameter(HttpClientParams.ALLOW_CIRCULAR_REDIRECTS,Boolean.TRUE);
		client.setParams(params);
		GetMethod method = new GetMethod(url);
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

		try {

			int rc = client.executeMethod(method);

			if (rc != HttpStatus.SC_OK) {
				throw new IOException("Failed to read URL");
			} 
			InputStream input = method.getResponseBodyAsStream();
			OutputStream output = new FileOutputStream(outfile);
			stream(input, output);
		} finally {

			method.releaseConnection();
		}

	}
	public String executeGetMethodAndReturnResult(String url) throws IOException, HttpException {
		
		HttpClient client = new HttpClient();
		HttpClientParams params = client.getParams();
		params.setParameter(HttpClientParams.SO_TIMEOUT, 400000);
		params.setParameter(HttpClientParams.ALLOW_CIRCULAR_REDIRECTS,Boolean.TRUE);
		client.setParams(params);
        GetMethod method = new GetMethod(url);
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

		try {

			int rc = client.executeMethod(method);

			if (rc != HttpStatus.SC_OK) {
				
				throw new IOException("Failed to read URL");

			}
			return method.getResponseBodyAsString();
		} catch (Exception e) {
			throw new IOException(e.getMessage());
		}
		finally {

			method.releaseConnection();
		}
	}
	/**
	 * Makes HTTP GET request and writes result to response output stream.
	 * @param request fully qualified request URL.
	 * @param output OutputStream to write to
	 * @throws IOException
	 * @throws HttpException
	 */
	public void executeGetMethodAndStreamResult(String request, OutputStream output) throws IOException, HttpException {

		HttpClient client = new HttpClient();

		GetMethod method = new GetMethod(request);
		HttpClientParams params = client.getParams();
		params.setParameter(HttpClientParams.SO_TIMEOUT, 400000);
		params.setParameter(HttpClientParams.ALLOW_CIRCULAR_REDIRECTS,Boolean.TRUE);
		client.setParams(params);
		method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

		try {

			int rc = client.executeMethod(method);

			if (rc != HttpStatus.SC_OK) {

				throw new IOException("Failed to read URL");

			}

			streamGetMethodResponse(method,output);
		}
		finally {

			method.releaseConnection();
		}

	}

	public void streamGetMethodResponse(GetMethod method, OutputStream output) throws IOException, HttpException {

		InputStream input = method.getResponseBodyAsStream();
		stream(input, output);

	}

	public void stream(InputStream input, OutputStream output) throws IOException {
		byte[] buffer = new byte[streamBufferSize];
		int count = input.read(buffer);

		while( count != -1 && count <= streamBufferSize ) {

			output.write(buffer,0,count);
			count = input.read(buffer);
		}

		input.close();
		output.close();
	}
}
