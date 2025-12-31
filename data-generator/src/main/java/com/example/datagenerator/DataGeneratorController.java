package com.example.datagenerator;

import com.example.common.CommonService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DataGeneratorController {

    @GetMapping("/")
    public String hello() {
        CommonService commonService = new CommonService();
        return "Hello World from Data Generator! " + commonService.getMessage();
    }
}