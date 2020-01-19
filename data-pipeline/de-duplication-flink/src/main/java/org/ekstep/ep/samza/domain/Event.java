package org.ekstep.ep.samza.domain;

import org.apache.commons.lang3.StringUtils;
import org.ekstep.ep.samza.events.domain.Events;

import java.util.HashMap;
import java.util.Map;

public class Event extends Events {

    public Event(Map<String, Object> map) {
        super(map);
    }

    public void markSkipped() {
        telemetry.addFieldIfAbsent("flags", new HashMap<String, Boolean>());
        telemetry.add("flags.dd_processed", false);
        telemetry.add("flags.dd_checksum_present", false);
    }

    public void markDuplicate() {
        telemetry.addFieldIfAbsent("flags", new HashMap<String, Boolean>());
        telemetry.add("flags.dd_processed", false);
        telemetry.add("flags.dd_duplicate_event", true);
    }

    public void markSuccess() {
        telemetry.addFieldIfAbsent("flags", new HashMap<String, Boolean>());
        telemetry.add("flags.dd_processed", true);
        telemetry.add("type", "events");
    }

    public void markRedisFailure() {
        telemetry.addFieldIfAbsent("flags", new HashMap<String, Boolean>());
        telemetry.add("flags.dd_processed", false);
        telemetry.add("flags.dd_redis_failure", true);
        telemetry.add("type", "events");
    }

    // public void updateDefaults(DeDuplicationConfig config) {
    public void updateDefaults() {
        String channelString = telemetry.<String>read("context.channel").value();
        String channel = StringUtils.deleteWhitespace(channelString);
        if (channel == null || channel.isEmpty()) {
            telemetry.addFieldIfAbsent("context", new HashMap<String, Object>());
            telemetry.add("context.channel", "test");
        }
    }
}

