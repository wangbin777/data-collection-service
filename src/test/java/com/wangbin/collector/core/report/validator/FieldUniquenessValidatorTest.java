package com.wangbin.collector.core.report.validator;

import com.wangbin.collector.common.domain.entity.DataPoint;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FieldUniquenessValidatorTest {

    private DataPoint createPoint(String pointId, String alias) {
        DataPoint point = new DataPoint();
        point.setDeviceId("dev-1");
        point.setPointId(pointId);
        point.setPointAlias(alias);
        Map<String, Object> config = new HashMap<>();
        config.put("reportEnabled", true);
        point.setAdditionalConfig(config);
        return point;
    }

    @Test
    void duplicateReportFieldWillDowngradeToRawOnly() {
        FieldUniquenessValidator validator = new FieldUniquenessValidator();
        DataPoint p1 = createPoint("p1", "ia");
        DataPoint p2 = createPoint("p2", "ia");

        assertTrue(p1.isReportEnabled());
        assertTrue(p2.isReportEnabled());

        validator.validate("dev-1", List.of(p1, p2));

        assertFalse(p1.isReportEnabled(), "conflicted point should not be reported");
        assertFalse(p2.isReportEnabled(), "conflicted point should not be reported");
    }
}
