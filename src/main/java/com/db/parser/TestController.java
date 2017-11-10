package com.db.parser;

import java.io.IOException;
import java.util.List;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;


@Controller
public class TestController {

	@RequestMapping(value = "/")
	public ModelAndView testMe(ModelAndView model) throws IOException {
		model.setViewName("index");
		return model;
	}
}
