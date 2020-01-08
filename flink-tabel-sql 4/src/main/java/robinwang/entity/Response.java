package robinwang.entity;

/**
 * JavaBean类
 * JSON:
 * {
 * 	"response": "",
 * 	"status": 0,
 * 	"protocol": ""
 * 	"timestamp":0
 * }
 */
public class Response {
    private String response;
    private int status;
    private String protocol;
    private long timestamp;

    public Response(String response, int status, String protocol, long timestamp) {
        this.response = response;
        this.status = status;
        this.protocol = protocol;
        this.timestamp = timestamp;
    }
    public Response(){}

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
