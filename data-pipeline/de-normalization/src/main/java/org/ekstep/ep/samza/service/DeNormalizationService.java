package org.ekstep.ep.samza.service;

import com.google.gson.JsonSyntaxException;
import org.ekstep.ep.samza.core.Logger;
import org.ekstep.ep.samza.domain.Event;
import org.ekstep.ep.samza.task.DeNormalizationConfig;
import org.ekstep.ep.samza.task.DeNormalizationSink;
import org.ekstep.ep.samza.task.DeNormalizationSource;
import org.ekstep.ep.samza.util.*;

import java.util.List;
import java.util.Map;

import static java.text.MessageFormat.format;

public class DeNormalizationService {

    static Logger LOGGER = new Logger(DeNormalizationService.class);
    private final DeNormalizationConfig config;
    private final DeviceDataCache deviceCache;
    private final UserDataCache userCache;
    private final ContentDataCache contentCache;
    private final DialCodeDataCache dialcodeCache;
    private final RedisConnect redisConnect;

    public DeNormalizationService(DeNormalizationConfig config, DeviceDataCache deviceCache, RedisConnect redisConnect, UserDataCache userCache, ContentDataCache contentCache, DialCodeDataCache dialcodeCache) {
        this.config = config;
        this.deviceCache = deviceCache;
        this.userCache = userCache;
        this.contentCache = contentCache;
        this.dialcodeCache = dialcodeCache;
        this.redisConnect = redisConnect;
    }

    public void process(DeNormalizationSource source, DeNormalizationSink sink) {
        Event event = null;
        try {
            event = source.getEvent();
            // add device details to the event
            event = updateEventWithDeviceData(event);
            // add user details to the event
            event = updateEventWithUserData(event);
            // add content details to the event
            event = updateEventWithObjectData(event);
            // add dialcode details to the event
            event = updateEventWithDialCodeData(event);
            sink.toSuccessTopic(event);
        } catch(JsonSyntaxException e){
            LOGGER.error(null, "INVALID EVENT: " + source.getMessage());
            sink.toMalformedTopic(source.getMessage());
        } catch (Exception e) {
            LOGGER.error(null,
                    format("EXCEPTION. PASSING EVENT THROUGH AND ADDING IT TO EXCEPTION TOPIC. EVENT: {0}, EXCEPTION:",
                            event),
                    e);
            sink.toErrorTopic(event, e.getMessage());
        }


    }

    private Event updateEventWithDeviceData(Event event) {

        Map device;
        try {
            String did = event.did();
            String channel = event.channel();
            if (did != null && !did.isEmpty()) {
                device = deviceCache.getDataForDeviceId(event.did(), channel);

                if (device != null && !device.isEmpty()) {
                    event.addDeviceData(device);
                }
                else {
                    event.setFlag(DeNormalizationConfig.getDeviceLocationJobFlag(), false);
                }
            }
            return event;
        } catch(Exception ex) {
            LOGGER.error(null,
                    format("EXCEPTION. EVENT: {0}, EXCEPTION:",
                            event),
                    ex);
            return event;
        }
    }

    private Event updateEventWithUserData(Event event) {

        Map user;
        try {
            String userId = event.actorId();
            String userType = event.actorType();
            if (userId != null && !userId.isEmpty()) {
                if(!userType.equalsIgnoreCase("system")) {
                    user = userCache.getDataForUserId(userId);
                    if (user != null && !user.isEmpty()) {
                        event.addUserData(user);
                    } else {
                        event.setFlag(DeNormalizationConfig.getUserLocationJobFlag(), false);
                    }
                }
            }
            return event;
        } catch(Exception ex) {
            LOGGER.error(null,
                    format("EXCEPTION. EVENT: {0}, EXCEPTION:",
                            event),
                    ex);
            return event;
        }
    }

    private Event updateEventWithObjectData(Event event) {

        Map content;
        try {
            String contentId = event.objectID();
            if (contentId != null && !contentId.isEmpty()) {
                content = contentCache.getDataForContentId(contentId);
                if (content != null && !content.isEmpty()) {
                    event.addContentData(content);
                }
                else {
                    event.setFlag(DeNormalizationConfig.getContentLocationJobFlag(), false);
                }
            }
            return event;
        } catch(Exception ex) {
            LOGGER.error(null,
                    format("EXCEPTION. EVENT: {0}, EXCEPTION:",
                            event),
                    ex);
            return event;
        }
    }

    private Event updateEventWithDialCodeData(Event event) {

        List<Map> dailcodeData;
        try {
            List<String> dialcodes = event.dialCode();
            if (dialcodes != null && !dialcodes.isEmpty()) {
                dailcodeData = dialcodeCache.getDataForDialCodes(dialcodes);
                if (dailcodeData != null && !dailcodeData.isEmpty()) {
                    event.addDialCodeData(dailcodeData);
                }
                else {
                    event.setFlag(DeNormalizationConfig.getDialCodeLocationJobFlag(), false);
                }
            }
            return event;
        } catch(Exception ex) {
            LOGGER.error(null,
                    format("EXCEPTION. EVENT: {0}, EXCEPTION:",
                            event),
                    ex);
            return event;
        }
    }

}