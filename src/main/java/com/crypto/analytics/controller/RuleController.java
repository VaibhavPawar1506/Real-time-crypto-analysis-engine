package com.crypto.analytics.controller;

import com.crypto.analytics.config.DynamicRuleConfig;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/rules")
public class RuleController {

    private final DynamicRuleConfig dynamicRuleConfig;

    public RuleController(DynamicRuleConfig dynamicRuleConfig) {
        this.dynamicRuleConfig = dynamicRuleConfig;
    }

    @GetMapping("/threshold")
    public double getThreshold() {
        return dynamicRuleConfig.getVolatilityThreshold();
    }

    @PostMapping("/threshold")
    public void updateThreshold(@RequestParam double newThreshold) {
        dynamicRuleConfig.setVolatilityThreshold(newThreshold);
    }
}
