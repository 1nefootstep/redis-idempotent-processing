package com.ocean.redisidempotentprocessing;

import com.ocean.redisidempotentprocessing.event.AssetEvent;
import com.ocean.redisidempotentprocessing.util.JsonUtil;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class GenerateAssetBenchmarkTestData {
    public static void main(String[] args) {
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();

        List<AssetEvent> result = threadLocalRandom.doubles(30000, 1d, 3000d).mapToObj(
                        expense -> {
                            String ownerId = String.valueOf(threadLocalRandom.nextInt(1, 10));
                            String businessId = String.valueOf(threadLocalRandom.nextInt(1, 2000));

                            return new AssetEvent(
                                    ownerId,
                                    businessId,
                                    String.valueOf(expense));
                        })
                .toList();

        try (FileWriter fileWriter = new FileWriter("src/main/resources/benchmark/scenario1-9-2000.json")) {
            fileWriter.write(JsonUtil.toJSONString(result));
        } catch (IOException e) {
            e.printStackTrace();
        }
//        var map = Map.of(
//                "1", new BigDecimal("742881.4995116135120230"),
//                "2", new BigDecimal("777978.5204638965633700"),
//                "3", new BigDecimal("758883.2122384480055810"),
//                "4", new BigDecimal("756309.0616121380033210"),
//                "5", new BigDecimal("706042.5705404768929790"),
//                "6", new BigDecimal("759360.2463226797730150"),
//                "7", new BigDecimal("742615.2134240574812795"),
//                "8", new BigDecimal("753857.7055701255617225"),
//                "9", new BigDecimal("751938.0799903697862920")
//        );
//        System.out.println(JsonUtil.toJSONString(map));
    }
}
