//package com.lincoln.lin.kafkastudy.wechat.common;
//
//import java.io.IOException;
//import org.springframework.context.annotation.Configuration;
//
///**
// * description:
// *
// * @author linye
// * @date 2022年03月23日 3:36 下午
// */
//@WebFilter(filterName = "CorsFilter")
//@Configuration
//public class CorsFilter implements Filter {
//  @Override
//  public void doFilter(ServletRequest req, ServletResponse res, FilterChain chain) throws IOException, ServletException {
//    HttpServletResponse response = (HttpServletResponse) res;
//    response.setHeader("Access-Control-Allow-Origin","*");
//    response.setHeader("Access-Control-Allow-Credentials", "true");
//    response.setHeader("Access-Control-Allow-Methods", "POST, GET, PATCH, DELETE, PUT");
//    response.setHeader("Access-Control-Max-Age", "3600");
//    response.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
//    chain.doFilter(req, res);
//  }
//}
