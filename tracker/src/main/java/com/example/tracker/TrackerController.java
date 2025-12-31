package com.example.tracker;

import com.example.common.CommonService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TrackerController {

    @GetMapping("/")
    public String hello() {
        CommonService commonService = new CommonService();
        return "Hello World from Tracker! " + commonService.getMessage();
    }
}