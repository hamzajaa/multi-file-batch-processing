package com.example.batch.bean;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
public class Customer {
    @Id
    Long id;
    String firstName;
    String lastName;
    String email;
    String gender;
    String contactNo;
    String country;
    String dob;

}
