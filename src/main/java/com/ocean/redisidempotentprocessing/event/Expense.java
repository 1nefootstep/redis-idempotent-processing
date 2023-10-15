package com.ocean.redisidempotentprocessing.event;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Expense {
    @JsonProperty("ownerId")
    private String ownerId;
    @JsonProperty("businessId")
    private String businessId;
    @JsonProperty("expense")
    private String expense;
}