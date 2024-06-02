package com.example.batch.service;

import com.example.batch.bean.Customer;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;

public interface CustomerService {

   void importCustomers(MultipartFile file) throws Exception;
}
