package com.ocean.redisidempotentprocessing.service;

import com.ocean.redisidempotentprocessing.event.Expense;

import java.math.BigDecimal;

public interface OwnerExpenseService {
    String strategyName();

    BigDecimal updateExpenseAndAggregate(Expense expense);

    BigDecimal aggregate(String ownerId);
}
