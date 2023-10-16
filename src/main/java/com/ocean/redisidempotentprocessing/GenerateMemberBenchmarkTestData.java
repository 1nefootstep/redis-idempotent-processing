package com.ocean.redisidempotentprocessing;

import com.ocean.redisidempotentprocessing.event.MemberEvent;
import com.ocean.redisidempotentprocessing.util.JsonUtil;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class GenerateMemberBenchmarkTestData {
    public static void main(String[] args) {
        ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
        Map<String, Map<String, Set<String>>> ownerMap = new HashMap<>();
        // 1000 users, 9 owners, 100 businesses

        List<MemberEvent> result = threadLocalRandom.ints(30000, 1, 1001).mapToObj(
                        member -> {
                            int i = threadLocalRandom.nextInt(1, 11);
                            boolean isRemove = ownerMap.size() > 0 && i > 7;
                            if (isRemove) {
                                try {
                                    List<Map.Entry<String, Map<String, Set<String>>>> ls1 = ownerMap.entrySet().stream()
                                            .filter(e -> e.getValue().values().stream().anyMatch(s -> !s.isEmpty())).collect(Collectors.toList());
                                    Collections.shuffle(ls1);
                                    Map.Entry<String, Map<String, Set<String>>> e1 = ls1.get(0);
                                    String ownerId = e1.getKey();
                                    List<Map.Entry<String, Set<String>>> ls2 = e1.getValue().entrySet().stream()
                                            .filter(e -> !e.getValue().isEmpty())
                                            .collect(Collectors.toList());
                                    Collections.shuffle(ls2);
                                    Map.Entry<String, Set<String>> e2 = ls2.get(0);
                                    String businessId = e2.getKey();
                                    Set<String> memberSet = e2.getValue();
                                    String memberId = memberSet.stream().findAny().get();
                                    memberSet.remove(memberId);
                                    return new MemberEvent(
                                            ownerId,
                                            businessId,
                                            memberId,
                                            MemberEvent.Status.NOT_MEMBER
                                    );
                                } catch (Exception e) {
                                    System.out.println(e);
                                }
                            }
                            String ownerId = String.valueOf(threadLocalRandom.nextInt(1, 10));
                            String businessId = String.valueOf(threadLocalRandom.nextInt(1, 101));
                            String memberId = String.valueOf(member);

                            boolean isMember = ownerMap.computeIfAbsent(ownerId, x -> new HashMap<>())
                                    .computeIfAbsent(businessId, x -> new HashSet<>())
                                    .contains(memberId);
                            if (isMember) {
                                ownerMap.computeIfAbsent(ownerId, x -> new HashMap<>())
                                        .computeIfAbsent(businessId, x -> new HashSet<>())
                                        .remove(memberId);
                            } else {
                                ownerMap.computeIfAbsent(ownerId, x -> new HashMap<>())
                                        .computeIfAbsent(businessId, x -> new HashSet<>())
                                        .add(memberId);
                            }
                            return new MemberEvent(
                                    ownerId,
                                    businessId,
                                    memberId,
                                    isMember ? MemberEvent.Status.NOT_MEMBER : MemberEvent.Status.MEMBER
                            );
                        })
                .toList();

        try (FileWriter fileWriter = new FileWriter("src/main/resources/benchmark/scenario2-1000-9-500.json")) {
            fileWriter.write(JsonUtil.toJSONString(result));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (FileWriter fileWriter = new FileWriter("src/main/resources/benchmark/scenario2-1000-9-500-result.json")) {
            Map<String, Integer> map = new HashMap<>();
            for (var e : ownerMap.entrySet()) {
                String ownerId = e.getKey();
                Map<String, Set<String>> businessIdToMemberSet = e.getValue();
                Set<String> ownerMemberSet = new HashSet<>();
                for (var e2 : businessIdToMemberSet.entrySet()) {
                    String businessId = e2.getKey();
                    Set<String> businessMemberSet = e2.getValue();
                    ownerMemberSet.addAll(businessMemberSet);
                }
                map.put(ownerId, ownerMemberSet.size());
            }

            fileWriter.write(JsonUtil.toJSONString(map));
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
