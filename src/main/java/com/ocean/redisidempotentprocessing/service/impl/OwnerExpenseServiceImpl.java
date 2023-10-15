package com.ocean.redisidempotentprocessing.service.impl;

import com.ocean.redisidempotentprocessing.event.Expense;
import com.ocean.redisidempotentprocessing.service.OwnerExpenseService;
import com.ocean.redisidempotentprocessing.service.SumRedisHashStrategy;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class OwnerExpenseServiceImpl implements OwnerExpenseService {
    private final SumRedisHashStrategy sumRedisHashStrategy;
    private static final String REDIS_KEY_TEMPLATE = "ocean:expense:{owner:%s}";

    public OwnerExpenseServiceImpl(SumRedisHashStrategy sumRedisHashStrategy) {
        this.sumRedisHashStrategy = sumRedisHashStrategy;
    }

    @Override
    public String strategyName() {
        return sumRedisHashStrategy.strategyName();
    }

    @Override
    public BigDecimal updateExpenseAndAggregate(Expense expense) {
        String redisKey = this.getRedisKey(expense);
        BigDecimal bdExpense = new BigDecimal(expense.getExpense()).setScale(16, RoundingMode.FLOOR);
        return sumRedisHashStrategy.setAndSum(redisKey, expense.getBusinessId(), bdExpense);
    }

    @Override
    public BigDecimal aggregate(String ownerId) {
        return sumRedisHashStrategy.sum(this.getRedisKey(ownerId));
    }

    private String getRedisKey(Expense expense) {
        return this.getRedisKey(expense.getOwnerId());
    }

    private String getRedisKey(String ownerId) {
        return String.format(REDIS_KEY_TEMPLATE, ownerId);
    }
}
