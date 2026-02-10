package com.lucid.automation.airouting.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lucid.automation.airouting.config.TopicVisibilityProperties;
import com.lucid.automation.airouting.dto.topic.TopicActionItem;
import com.lucid.automation.airouting.dto.topic.TopicMetadata;
import com.lucid.automation.airouting.dto.topic.TopicMetadataEvent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

public class TopicVisibilityServiceTest {

    @Test
    void buildVisibilityEvents_filtersByOpenOwnerAndChannelMembership() {
        TopicVisibilityProperties props = new TopicVisibilityProperties();
        props.setStrictChannelMembership(true);
        props.setUserChannelsJson("""
            { "alice": ["#campaign-briefs"], "bob": ["#project-updates"] }
            """);

        ChannelMembershipService membership = new ChannelMembershipService(props, new ObjectMapper());
        TopicVisibilityService service = new TopicVisibilityService(membership, props);

        TopicActionItem openAlice = new TopicActionItem();
        openAlice.setOwner("alice");
        openAlice.setStatus("OPEN");
        openAlice.setTask("Send invoice");

        TopicActionItem doneBob = new TopicActionItem();
        doneBob.setOwner("bob");
        doneBob.setStatus("DONE");
        doneBob.setTask("Already done");

        TopicMetadata topic = new TopicMetadata();
        topic.setChannel("#campaign-briefs");
        topic.setActionItems(List.of(openAlice, doneBob));

        TopicMetadataEvent ev = new TopicMetadataEvent();
        ev.setEventId("e1");
        ev.setCreatedAt(Instant.now());
        ev.setWorkspaceId("ws-gemini");
        ev.setBatchId("ws-gemini:batch_1");
        ev.setTopicId("11111111-1111-1111-1111-111111111111");
        ev.setTopic(topic);

        var out = service.computeVisibility(ev);
        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals("alice", out.get(0).userId());
        Assertions.assertEquals("11111111-1111-1111-1111-111111111111", out.get(0).topicId());
    }

    @Test
    void buildVisibilityEvents_supportsAliasFallback_whenOwnerIsDisplayName() {
        TopicVisibilityProperties props = new TopicVisibilityProperties();
        props.setStrictChannelMembership(true);
        props.setUserChannelsJson("""
            { "U123": ["#campaign-briefs"] }
            """);
        props.setIdentityAliasesJson("""
            { "Devon": "U123" }
            """);

        ChannelMembershipService membership = new ChannelMembershipService(props, new ObjectMapper());
        TopicVisibilityService service = new TopicVisibilityService(membership, props);

        TopicActionItem open = new TopicActionItem();
        open.setOwner("Devon");
        open.setStatus("OPEN");
        open.setTask("Send invoice");

        TopicMetadata topic = new TopicMetadata();
        topic.setChannel("#campaign-briefs");
        topic.setActionItems(List.of(open));

        TopicMetadataEvent ev = new TopicMetadataEvent();
        ev.setEventId("e2");
        ev.setCreatedAt(Instant.now());
        ev.setWorkspaceId("ws-gemini");
        ev.setBatchId("ws-gemini:batch_2");
        ev.setTopicId("22222222-2222-2222-2222-222222222222");
        ev.setTopic(topic);

        var out = service.computeVisibility(ev);
        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals("U123", out.get(0).userId());
    }

    @Test
    void buildVisibilityEvents_prefersOwnerUserId_whenProvided() {
        TopicVisibilityProperties props = new TopicVisibilityProperties();
        props.setStrictChannelMembership(true);
        props.setUserChannelsJson("""
            { "U555": ["#project-updates"] }
            """);

        ChannelMembershipService membership = new ChannelMembershipService(props, new ObjectMapper());
        TopicVisibilityService service = new TopicVisibilityService(membership, props);

        TopicActionItem open = new TopicActionItem();
        open.setOwner("Someone");
        open.setOwnerUserId("U555");
        open.setStatus("OPEN");
        open.setTask("Confirm shipping date");

        TopicMetadata topic = new TopicMetadata();
        topic.setChannel("#project-updates");
        topic.setActionItems(List.of(open));

        TopicMetadataEvent ev = new TopicMetadataEvent();
        ev.setEventId("e3");
        ev.setCreatedAt(Instant.now());
        ev.setWorkspaceId("ws-gemini");
        ev.setBatchId("ws-gemini:batch_3");
        ev.setTopicId("33333333-3333-3333-3333-333333333333");
        ev.setTopic(topic);

        var out = service.computeVisibility(ev);
        Assertions.assertEquals(1, out.size());
        Assertions.assertEquals("U555", out.get(0).userId());
    }
}
