/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.controller;

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;

import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER_CHANGE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for KAFKA-19508: Producer keeps reporting a NotLeaderOrFollowerException
 * 
 * This test class reproduces the "zombie leader" scenario where leader changes
 * but the leader epoch doesn't increment, causing Producer to receive
 * NotLeaderOrFollowerException continuously.
 */
@Timeout(value = 40)
public class PartitionChangeBuilderKafka19508Test {

    private static final Uuid TOPIC_ID = Uuid.fromString("FbrrdcfiR-KC2CPSTHaJrg");
    
    /**
     * Helper method to safely merge partition change record using reflection
     */
    private PartitionRegistration mergePartitionChange(PartitionRegistration partition, Object changeRecord) {
        try {
            Method mergeMethod = PartitionRegistration.class.getMethod("merge", 
                Class.forName("org.apache.kafka.common.metadata.PartitionChangeRecord"));
            return (PartitionRegistration) mergeMethod.invoke(partition, changeRecord);
        } catch (Exception e) {
            throw new RuntimeException("Failed to merge partition change record", e);
        }
    }
    
    /**
     * Helper method to get leader field from change record using reflection
     */
    private int getLeaderFromRecord(Object changeRecord) {
        try {
            Method getLeader = changeRecord.getClass().getMethod("leader");
            return (Integer) getLeader.invoke(changeRecord);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get leader from record", e);
        }
    }
    
    /**
     * Test reproducing KAFKA-19508: Zombie leader scenario where leader changes
     * but epoch doesn't increment due to missing record.setLeader() call.
     * 
     * This simulates the scenario where:
     * 1. Initial leader is broker 12 with epoch 100
     * 2. Leader changes to broker 11 but epoch remains 100 (the bug)
     * 3. Producer continues to send to broker 12, gets NotLeaderOrFollowerException
     */
    @Test
    public void testZombieLeaderScenarioReproduction() {
        // Create initial partition state: leader=12, epoch=100
        PartitionRegistration initialPartition = new PartitionRegistration.Builder()
            .setReplicas(new int[] {12, 11, 13})
            .setDirectories(new Uuid[]{
                Uuid.fromString("dpdvA5AZSWySmnPFTnu5Kw"),
                Uuid.fromString("V60B3cglScq3Xk8BX1NxAQ"),
                DirectoryId.UNASSIGNED
            })
            .setIsr(new int[] {12, 11, 13})
            .setLeader(12)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
            .setLeaderEpoch(100)
            .setPartitionEpoch(200)
            .build();

        // Create builder with broker 12 unavailable (simulating server restart/failure)
        PartitionChangeBuilder builder = new PartitionChangeBuilder(
            initialPartition,
            TOPIC_ID,
            0,
            brokerId -> brokerId != 12, // broker 12 is not acceptable (unavailable)
            MetadataVersion.IBP_3_5_IV2, // Use 3.5.0 version where the bug occurs
            2 // minISR
        );

        // Build the change record - this should trigger leader election
        Optional<ApiMessageAndVersion> result = builder.build();
        
        assertTrue(result.isPresent(), "Expected a partition change record to be generated");
        
        // Get the record and verify leader change
        Object changeRecord = result.get().message();
        int newLeader = getLeaderFromRecord(changeRecord);
        
        // Verify that leader changed from 12 to 11
        assertEquals(11, newLeader, "Expected new leader to be broker 11");
        assertNotEquals(12, newLeader, "Leader should have changed from broker 12");
        
        // Apply the change to get the new partition state
        PartitionRegistration newPartition = mergePartitionChange(initialPartition, changeRecord);
        
        // This is the key assertion: leader epoch MUST increment when leader changes
        // Before the fix, this would fail because epoch would remain 100
        assertEquals(101, newPartition.leaderEpoch, 
            "Leader epoch must increment when leader changes to prevent zombie leader scenario");
        assertEquals(11, newPartition.leader, "New leader should be broker 11");
    }

    /**
     * Test the scenario where ISR shrinks and leader changes simultaneously.
     * This tests the interaction between ISR shrink epoch bump logic and leader change.
     */
    @ParameterizedTest
    @ValueSource(strings = {"3.4-IV0", "3.5-IV2", "3.6-IV0"})
    public void testIsrShrinkWithLeaderChangeEpochBump(String metadataVersionString) {
        MetadataVersion metadataVersion = MetadataVersion.fromVersionString(metadataVersionString);
        
        // Create partition with leader=12, ISR=[12, 11, 13]
        PartitionRegistration partition = new PartitionRegistration.Builder()
            .setReplicas(new int[] {12, 11, 13})
            .setDirectories(new Uuid[]{
                Uuid.fromString("dpdvA5AZSWySmnPFTnu5Kw"),
                Uuid.fromString("V60B3cglScq3Xk8BX1NxAQ"),
                DirectoryId.UNASSIGNED
            })
            .setIsr(new int[] {12, 11, 13})
            .setLeader(12)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
            .setLeaderEpoch(100)
            .setPartitionEpoch(200)
            .build();

        // Simulate scenario: broker 12 and 13 become unavailable, ISR shrinks to [11]
        PartitionChangeBuilder builder = new PartitionChangeBuilder(
            partition,
            TOPIC_ID,
            0,
            brokerId -> brokerId == 11, // Only broker 11 is acceptable
            metadataVersion,
            2
        ).setTargetIsr(List.of(11)); // ISR shrinks to just broker 11

        Optional<ApiMessageAndVersion> result = builder.build();
        
        assertTrue(result.isPresent(), "Expected a partition change record");
        
        Object changeRecord = result.get().message();
        PartitionRegistration newPartition = mergePartitionChange(partition, changeRecord);
        
        // Verify leader changed and epoch incremented
        assertEquals(11, newPartition.leader, "Leader should change to broker 11");
        assertEquals(101, newPartition.leaderEpoch, 
            "Leader epoch must increment when leader changes during ISR shrink");
        assertArrayEquals(new int[]{11}, newPartition.isr, "ISR should shrink to [11]");
    }

    /**
     * Test that when leader doesn't change, epoch should not increment.
     * This ensures our fix doesn't cause unnecessary epoch bumps.
     */
    @Test
    public void testNoEpochBumpWhenLeaderUnchanged() {
        // Create partition with leader=11
        PartitionRegistration partition = new PartitionRegistration.Builder()
            .setReplicas(new int[] {11, 12, 13})
            .setDirectories(new Uuid[]{
                Uuid.fromString("dpdvA5AZSWySmnPFTnu5Kw"),
                Uuid.fromString("V60B3cglScq3Xk8BX1NxAQ"),
                DirectoryId.UNASSIGNED
            })
            .setIsr(new int[] {11, 12})
            .setLeader(11)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
            .setLeaderEpoch(100)
            .setPartitionEpoch(200)
            .build();

        // Only change ISR, leader remains the same
        PartitionChangeBuilder builder = new PartitionChangeBuilder(
            partition,
            TOPIC_ID,
            0,
            brokerId -> true, // All brokers acceptable
            MetadataVersion.IBP_3_5_IV2,
            2
        ).setTargetIsr(List.of(11, 12, 13)); // Expand ISR

        Optional<ApiMessageAndVersion> result = builder.build();
        
        if (result.isPresent()) {
            Object changeRecord = result.get().message();
            int leaderField = getLeaderFromRecord(changeRecord);
            
            // Leader should not change
            assertEquals(NO_LEADER_CHANGE, leaderField, 
                "Leader should not change when only ISR expands");
            
            PartitionRegistration newPartition = mergePartitionChange(partition, changeRecord);
            
            // Epoch should remain the same since leader didn't change
            assertEquals(100, newPartition.leaderEpoch, 
                "Leader epoch should not increment when leader doesn't change");
            assertEquals(11, newPartition.leader, "Leader should remain broker 11");
        }
    }

    /**
     * Test the specific scenario mentioned in KAFKA-19508:
     * Leader changes from 12 to 11 but epoch doesn't increment.
     */
    @Test
    public void testKafka19508SpecificScenario() {
        // Reproduce the exact scenario from the bug report:
        // 04:34:01 - leader=12, epoch=N
        // 04:34:16 - leader=11, epoch=N (should be N+1)
        
        PartitionRegistration partition = new PartitionRegistration.Builder()
            .setReplicas(new int[] {12, 11, 13})
            .setDirectories(new Uuid[]{
                DirectoryId.UNASSIGNED,
                DirectoryId.UNASSIGNED,
                DirectoryId.UNASSIGNED
            })
            .setIsr(new int[] {12, 11})
            .setLeader(12)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
            .setLeaderEpoch(42) // Use specific epoch from bug report context
            .setPartitionEpoch(100)
            .build();

        // Simulate server restart scenario where broker 12 becomes unavailable
        PartitionChangeBuilder builder = new PartitionChangeBuilder(
            partition,
            TOPIC_ID,
            0,
            brokerId -> brokerId != 12, // Broker 12 unavailable after restart
            MetadataVersion.IBP_3_5_IV2,
            1
        );

        Optional<ApiMessageAndVersion> result = builder.build();
        
        assertTrue(result.isPresent(), "Expected partition change for leader election");
        
        Object changeRecord = result.get().message();
        PartitionRegistration newPartition = mergePartitionChange(partition, changeRecord);
        
        // Verify the fix: leader changes and epoch increments
        assertEquals(11, newPartition.leader, "Leader should change from 12 to 11");
        assertEquals(43, newPartition.leaderEpoch, 
            "Leader epoch must increment from 42 to 43 when leader changes");
        
        // This prevents the NotLeaderOrFollowerException scenario:
        // - Producer's cached metadata: leader=12, epoch=42
        // - Actual cluster state: leader=11, epoch=43
        // - When Producer sends to broker 12, it gets NotLeaderOrFollowerException
        // - Producer updates metadata and sees the epoch change, resolving the issue
    }
}