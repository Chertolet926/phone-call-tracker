package com.example.datagenerator.controller;

import com.example.common.CommonService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class DataGenerator {

    @GetMapping("/")
    public String hello() {
        CommonService commonService = new CommonService();
        return "Hello World from Data Generator! " + commonService.getMessage();
    }
}