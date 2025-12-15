package com.wangbin.collector.api.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class HealthController {


    /**
     * 健康检查
     * @return
     */
    @GetMapping("/health")
    public String health() {
       return "OK";
    }
}
