package part4;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class RtPayloadHeader {
    Long content_length;
    String originalTraceParent;
    String wm_qos_correlation_id;
    String wm_svc_name;
    String wm_ifx_client_type;
    String tenant_id;
    String wm_consumer_id;
    String wm_consumer_ip;
    String wm_sec_auth_token;
    String x_envoy_external_address;
    String wm_request_context;
    String x_o_bu;
    Long wm_consumer_intimestamp;
    String wm_client_app;
    String host;
    Long x_o_mart_id;
    String content_type;
    String wm_svc_env;
    String wm_expo_id;
    String x_request_id;
    String wm_client_env;
    String accept_language;
    String x_forwarded_proto;
    String wm_svc_version;
    String wm_consumer_source_type;
    String x_forwarded_for;
    String wm_client_usecase;
    String accept;
    Long x_envoy_attempt_count;
    String wm_vtc_id;
    String wm_client_region;
    String traceparent;
    String user_agent;

    // Constructor
    public RtPayloadHeader(
            @JsonProperty("content_length")
            Long content_length,
            @JsonProperty("originalTraceParent")
            String originalTraceParent,
            @JsonProperty("wm_qos_correlation_id")
            String wm_qos_correlation_id,
            @JsonProperty("wm_svc_name")
            String wm_svc_name,
            @JsonProperty("wm_ifx_client_type")
            String wm_ifx_client_type,
            @JsonProperty("tenant_id")
            String tenant_id,
            @JsonProperty("wm_consumer_id")
            String wm_consumer_id,
            @JsonProperty("wm_consumer_ip")
            String wm_consumer_ip,
            @JsonProperty("wm_sec_auth_token")
            String wm_sec_auth_token,
            @JsonProperty("x_envoy_external_address")
            String x_envoy_external_address,
            @JsonProperty("wm_request_context")
            String wm_request_context,
            @JsonProperty("x_o_bu")
            String x_o_bu,
            @JsonProperty("wm_consumer_intimestamp")
            Long wm_consumer_intimestamp,
            @JsonProperty("wm_client_app")
            String wm_client_app,
            @JsonProperty("host")
            String host,
            @JsonProperty("x_o_mart_id")
            Long x_o_mart_id,
            @JsonProperty("content_type")
            String content_type,
            @JsonProperty("wm_svc_env")
            String wm_svc_env,
            @JsonProperty("wm_expo_id")
            String wm_expo_id,
            @JsonProperty("x_request_id")
            String x_request_id,
            @JsonProperty("wm_client_env")
            String wm_client_env,
            @JsonProperty("accept_language")
            String accept_language,
            @JsonProperty("x_forwarded_proto")
            String x_forwarded_proto,
            @JsonProperty("wm_svc_version")
            String wm_svc_version,
            @JsonProperty("wm_consumer_source_type")
            String wm_consumer_source_type,
            @JsonProperty("x_forwarded_for")
            String x_forwarded_for,
            @JsonProperty("wm_client_usecase")
            String wm_client_usecase,
            @JsonProperty("accept")
            String accept,
            @JsonProperty("x_envoy_attempt_count")
            Long x_envoy_attempt_count,
            @JsonProperty("wm_vtc_id")
            String wm_vtc_id,
            @JsonProperty("wm_client_region")
            String wm_client_region,
            @JsonProperty("traceparent")
            String traceparent,
            @JsonProperty("user_agent")
            String user_agent) {
        this.content_length = content_length;
        this.originalTraceParent = originalTraceParent;
        this.wm_qos_correlation_id = wm_qos_correlation_id;
        this.wm_svc_name = wm_svc_name;
        this.wm_ifx_client_type = wm_ifx_client_type;
        this.tenant_id = tenant_id;
        this.wm_consumer_id = wm_consumer_id;
        this.wm_consumer_ip = wm_consumer_ip;
        this.wm_sec_auth_token = wm_sec_auth_token;
        this.x_envoy_external_address = x_envoy_external_address;
        this.wm_request_context = wm_request_context;
        this.x_o_bu = x_o_bu;
        this.wm_consumer_intimestamp = wm_consumer_intimestamp;
        this.wm_client_app = wm_client_app;
        this.host = host;
        this.x_o_mart_id = x_o_mart_id;
        this.content_type = content_type;
        this.wm_svc_env = wm_svc_env;
        this.wm_expo_id = wm_expo_id;
        this.x_request_id = x_request_id;
        this.wm_client_env = wm_client_env;
        this.accept_language = accept_language;
        this.x_forwarded_proto = x_forwarded_proto;
        this.wm_svc_version = wm_svc_version;
        this.wm_consumer_source_type = wm_consumer_source_type;
        this.x_forwarded_for = x_forwarded_for;
        this.wm_client_usecase = wm_client_usecase;
        this.accept = accept;
        this.x_envoy_attempt_count = x_envoy_attempt_count;
        this.wm_vtc_id = wm_vtc_id;
        this.wm_client_region = wm_client_region;
        this.traceparent = traceparent;
        this.user_agent = user_agent;
    }

    @Override
    public String toString() {
        return "RtPayloadHeader{" +
                "content_length=" + content_length +
                ", originalTraceParent='" + originalTraceParent + '\'' +
                ", wm_qos_correlation_id='" + wm_qos_correlation_id + '\'' +
                ", wm_svc_name='" + wm_svc_name + '\'' +
                ", wm_ifx_client_type='" + wm_ifx_client_type + '\'' +
                ", tenant_id='" + tenant_id + '\'' +
                ", wm_consumer_id='" + wm_consumer_id + '\'' +
                ", wm_consumer_ip='" + wm_consumer_ip + '\'' +
                ", wm_sec_auth_token='" + wm_sec_auth_token + '\'' +
                ", x_envoy_external_address='" + x_envoy_external_address + '\'' +
                ", wm_request_context='" + wm_request_context + '\'' +
                ", x_o_bu='" + x_o_bu + '\'' +
                ", wm_consumer_intimestamp=" + wm_consumer_intimestamp +
                ", wm_client_app='" + wm_client_app + '\'' +
                ", host='" + host + '\'' +
                ", x_o_mart_id=" + x_o_mart_id +
                ", content_type='" + content_type + '\'' +
                ", wm_svc_env='" + wm_svc_env + '\'' +
                ", wm_expo_id='" + wm_expo_id + '\'' +
                ", x_request_id='" + x_request_id + '\'' +
                ", wm_client_env='" + wm_client_env + '\'' +
                ", accept_language='" + accept_language + '\'' +
                ", x_forwarded_proto='" + x_forwarded_proto + '\'' +
                ", wm_svc_version='" + wm_svc_version + '\'' +
                ", wm_consumer_source_type='" + wm_consumer_source_type + '\'' +
                ", x_forwarded_for='" + x_forwarded_for + '\'' +
                ", wm_client_usecase='" + wm_client_usecase + '\'' +
                ", accept='" + accept + '\'' +
                ", x_envoy_attempt_count=" + x_envoy_attempt_count +
                ", wm_vtc_id='" + wm_vtc_id + '\'' +
                ", wm_client_region='" + wm_client_region + '\'' +
                ", traceparent='" + traceparent + '\'' +
                ", user_agent='" + user_agent + '\'' +
                '}';
    }
}
