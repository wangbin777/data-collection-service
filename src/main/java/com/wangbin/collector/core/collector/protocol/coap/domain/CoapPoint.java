package com.wangbin.collector.core.collector.protocol.coap.domain;

import lombok.Builder;
import lombok.Getter;

/**
 * 表示一个CoAP资源点。
 */
@Getter
@Builder
public class CoapPoint {
    private final String path;
    private final String fullUri;
    private final CoapMethod method;
    private final int mediaType;
    private final boolean observe;
    private final boolean binary;
    private final String query;

    public String resolveUri(String baseUri) {
        if (fullUri != null && !fullUri.isBlank()) {
            return appendQuery(fullUri);
        }
        String normalizedBase = baseUri.endsWith("/") ? baseUri.substring(0, baseUri.length() - 1) : baseUri;
        String normalizedPath = path.startsWith("/") ? path : "/" + path;
        return appendQuery(normalizedBase + normalizedPath);
    }

    private String appendQuery(String uri) {
        if (query == null || query.isBlank()) {
            return uri;
        }
        if (uri.contains("?")) {
            return uri + "&" + query;
        }
        return uri + "?" + query;
    }

    public boolean hasFullUri() {
        return fullUri != null && !fullUri.isBlank();
    }

    public String getObserveKey() {
        return hasFullUri() ? fullUri : path;
    }
}
