package com.lucid.automation.airouting.pipeline.postprocessing.step;

import com.lucid.automation.common.dto.enrichment.*;
import com.lucid.automation.airouting.model.SlackMessage;
import com.lucid.automation.airouting.model.User;
import com.lucid.automation.airouting.pipeline.postprocessing.PostProcessingContext;
import com.lucid.automation.airouting.service.ChannelService;
import com.lucid.automation.airouting.service.UserService;
import com.lucid.automation.airouting.util.TextUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Advanced pipeline step that performs comprehensive topic enrichment with user data
 * This step can be used in addition to or instead of the basic ConversationEnrichmentStep
 */
@Component
public class TopicEnrichmentStep implements PipelineStep {

    private static final Logger logger = LoggerFactory.getLogger(TopicEnrichmentStep.class);

    private static final String UNKNOWN_USER = "Unknown User";
    private static final String NOT_AVAILABLE = "N/A";
    private static final String DEFAULT_SUMMARY_FOR_PERSON = "No summary available";

    private final UserService userService;
    private final ChannelService channelService;

    public TopicEnrichmentStep(UserService userService, ChannelService channelService) {
        this.userService = userService;
        this.channelService = channelService;
    }

    @Override
    public PipelineStepResult execute(PostProcessingContext context) {
        long startTime = System.currentTimeMillis();
        logger.debug("Executing topic enrichment step");

        try {
            ConversationEnrichment existingEnrichment = context.getConversationEnrichment();
            if (existingEnrichment == null) {
                logger.warn("No existing conversation enrichment found, skipping topic enrichment");
                return PipelineStepResult.success("Skipped - no existing enrichment");
            }

            List<SlackMessage> messages = context.getRequestMessages();
            if (messages == null || messages.isEmpty()) {
                logger.warn("No messages found, skipping topic enrichment");
                return PipelineStepResult.success("Skipped - no messages");
            }

            String tenantId = context.getTenantId();
            String workspaceId = messages.isEmpty() ? null : messages.get(0).getWorkspaceId();

            // ✅ PERFORMANCE FIX: Batch load ALL users and channels ONCE at the beginning
            Map<String, EnrichmentUserDTO> cachedUsers = batchLoadAllUsers(messages, tenantId, workspaceId);
            long cacheLoadTime = System.currentTimeMillis() - startTime;
            logger.info("⚡ [PERFORMANCE] Loaded {} users in {}ms (avoiding {} individual DB calls)",
                cachedUsers.size(), cacheLoadTime, cachedUsers.size());

            // Enhance each topic with detailed information using cached data
            long enrichStartTime = System.currentTimeMillis();
            List<TopicEnrichment> enrichedTopics = existingEnrichment.topics().stream()
                .map(topic -> enhanceTopicWithUserData(topic, messages, context, cachedUsers))
                .collect(Collectors.toList());
            long enrichTime = System.currentTimeMillis() - enrichStartTime;

            // Create enhanced conversation enrichment
            ConversationEnrichment enhancedEnrichment = new ConversationEnrichment(
                enrichedTopics,
                existingEnrichment.participants(),
                existingEnrichment.messages(),
                existingEnrichment.metadata()
            );

            context.setConversationEnrichment(enhancedEnrichment);

            long totalTime = System.currentTimeMillis() - startTime;
            logger.info("✅ [PERFORMANCE] Topic enrichment completed in {}ms (cache: {}ms, enrich: {}ms) for {} topics",
                totalTime, cacheLoadTime, enrichTime, enrichedTopics.size());
            return PipelineStepResult.success("Topic enrichment completed successfully");

        } catch (Exception e) {
            long totalTime = System.currentTimeMillis() - startTime;
            logger.error("Error during topic enrichment after {}ms: {}", totalTime, e.getMessage(), e);
            return PipelineStepResult.success("Topic enrichment failed but continuing: " + e.getMessage());
        }
    }

    /**
     * ✅ PERFORMANCE FIX: Batch load all users from messages ONCE to avoid N+1 query problem
     * This replaces hundreds of individual DB calls with a single batch operation
     */
    private Map<String, EnrichmentUserDTO> batchLoadAllUsers(List<SlackMessage> messages, String tenantId, String workspaceId) {
        if (tenantId == null || workspaceId == null) {
            logger.warn("Cannot batch load users: tenantId={}, workspaceId={}", tenantId, workspaceId);
            return Map.of();
        }

        // Collect all unique user IDs from messages
        Set<String> allUserIds = messages.stream()
            .map(msg -> msg.getUniqueUserId() != null ? msg.getUniqueUserId() : msg.getUsername())
            .filter(Objects::nonNull)
            .filter(id -> !id.trim().isEmpty())
            .collect(Collectors.toSet());

        Map<String, EnrichmentUserDTO> userCache = new HashMap<>();

        // ✅ PERFORMANCE FIX: Batch load all users from database in one operation
        // Construct potential user IDs
        Set<String> potentialUserIds = new HashSet<>();
        Map<String, String> idToUserIdMap = new HashMap<>(); // Map generated ID back to original userId

        for (String userId : allUserIds) {
            String generatedId = User.generateId(tenantId, workspaceId, userId);
            potentialUserIds.add(generatedId);
            idToUserIdMap.put(generatedId, userId);
        }

        try {
            List<User> foundUsers = userService.getUsersByIds(potentialUserIds);

            for (User user : foundUsers) {
                if (user != null) {
                    // Map back to the original userId used in the message
                    String originalUserId = idToUserIdMap.get(user.getId());
                    if (originalUserId == null) {
                        // Fallback: try to match by uniqueUserId or slackUserId
                        if (allUserIds.contains(user.getUniqueUserId())) {
                            originalUserId = user.getUniqueUserId();
                        } else if (allUserIds.contains(user.getSlackUserId())) {
                            originalUserId = user.getSlackUserId();
                        }
                    }

                    if (originalUserId != null) {
                        String userName = getFieldValue(user, "name");
                        if (userName == null || userName.trim().isEmpty()) {
                            userName = originalUserId;
                        }
                        String displayName = getBestDisplayNameFromUser(user, userName);

                        EnrichmentUserDTO userDTO = new EnrichmentUserDTO(
                            originalUserId,
                            userName,
                            displayName,
                            getImageFromUser(user)
                        );
                        userCache.put(originalUserId, userDTO);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("❌ User Batch Lookup Failed: {}", e.getMessage());
        }

        // Fill in missing users with fallback
        for (String userId : allUserIds) {
            if (!userCache.containsKey(userId)) {
                // Fallback: try individual lookup if batch failed or ID generation mismatch
                try {
                    Optional<User> userOptional = userService.getUser(tenantId, workspaceId, userId);
                    if (userOptional.isPresent()) {
                        User user = userOptional.get();
                        String userName = getFieldValue(user, "name");
                        if (userName == null || userName.trim().isEmpty()) {
                            userName = userId;
                        }
                        String displayName = getBestDisplayNameFromUser(user, userName);
                        userCache.put(userId, new EnrichmentUserDTO(userId, userName, displayName, getImageFromUser(user)));
                    } else {
                        userCache.put(userId, new EnrichmentUserDTO(userId, userId, userId, null));
                    }
                } catch (Exception e) {
                    userCache.put(userId, new EnrichmentUserDTO(userId, userId, userId, null));
                }
            }
        }

        return userCache;
    }

    /**
     * ✅ PERFORMANCE FIX: Build user info map from pre-loaded cache (no DB calls)
     */
    private Map<String, EnrichmentUserDTO> buildUserInfoMapFromCache(List<SlackMessage> messages, Map<String, EnrichmentUserDTO> cachedUsers) {
        return messages.stream()
            .filter(msg -> {
                String key = msg.getUniqueUserId() != null ? msg.getUniqueUserId() : msg.getUsername();
                return key != null && !key.trim().isEmpty();
            })
            .collect(Collectors.toMap(
                msg -> msg.getUniqueUserId() != null ? msg.getUniqueUserId() : msg.getUsername(),
                msg -> {
                    String userId = msg.getUniqueUserId() != null ? msg.getUniqueUserId() : msg.getUsername();
                    // Return cached user or create basic fallback
                    return cachedUsers.getOrDefault(userId,
                        new EnrichmentUserDTO(
                            userId,
                            msg.getUsername() != null ? msg.getUsername() : userId,
                            getBestDisplayNameFromSlackMessage(msg),
                            msg.getAvatarUrl() != null ? msg.getAvatarUrl() :
                            msg.getImageOriginal() != null ? msg.getImageOriginal() : msg.getImage72()
                        ));
                },
                (existing, replacement) -> existing
            ));
    }

    private TopicEnrichment enhanceTopicWithUserData(TopicEnrichment topic, List<SlackMessage> messages,
                                                      PostProcessingContext context, Map<String, EnrichmentUserDTO> cachedUsers) {
        try {
            // Build user info map from cache (no DB calls)
            Map<String, EnrichmentUserDTO> userInfos = buildUserInfoMapFromCache(messages, cachedUsers);

            // Enhance text fields with user mentions
            String enhancedShortSummary = TextUtils.replaceSlackMentions(topic.shortSummary(), userInfos);
            String enhancedFullSummary = TextUtils.replaceSlackMentions(topic.fullSummary(), userInfos);
            String enhancedSuggestedAction = TextUtils.replaceSlackMentions(topic.suggestedAction(), userInfos);

            // Update period dates if null
            String periodStartDate = topic.periodStartDate();
            String periodEndDate = topic.periodEndDate();

            if (periodStartDate == null && !messages.isEmpty()) {
                periodStartDate = messages.get(0).getTimestamp().toString();
            }
            if (periodEndDate == null && !messages.isEmpty()) {
                periodEndDate = messages.get(messages.size() - 1).getTimestamp().toString();
            }

            String tenantId = context.getTenantId();
            String workspaceId = messages.isEmpty() ? null : messages.get(0).getWorkspaceId();

            // Extract enhanced people involved - use cached data (no DB calls)
            List<EnrichmentUserDTO> enhancedPeopleInvolved = enhancePeopleInvolvedFromCache(topic.peopleInvolved(), messages, cachedUsers);

            // Extract enhanced summary per person - use cached data (no DB calls)
            List<SummaryPerPerson> enhancedSummaryPerPerson = enhanceSummaryPerPersonFromCache(topic.summaryPerPerson(), messages, cachedUsers);

            // Extract enhanced suggested replies - minimal channel lookup only
            List<SuggestedReply> enhancedSuggestedReplies = enhanceSuggestedReplies(topic.suggestedReplies(), tenantId, workspaceId);

            // Calculate lastUpdated from the latest message timestamp
            LocalDateTime lastUpdated = messages.stream()
                .map(SlackMessage::getTimestamp)
                .filter(Objects::nonNull)
                .max(LocalDateTime::compareTo)
                .orElse(topic.lastUpdated());

            return new TopicEnrichment(
                topic.title(),
                enhancedShortSummary,
                enhancedFullSummary,
                enhancedSuggestedAction,
                topic.clientOrSupplier(),
                topic.deadline(),
                topic.urgency(),
                topic.category(),
                topic.subCategory(),
                topic.startTime(),
                topic.endTime(),
                periodStartDate,
                periodEndDate,
                topic.latestMessageDate(),
                lastUpdated,
                enhancedPeopleInvolved,
                enhancedSummaryPerPerson,
                topic.lastMessageDatePerPerson(),
                enhancedSuggestedReplies,
                topic.suggestedForwardRecipient()
            );

        } catch (Exception e) {
            logger.error("Failed to enhance topic '{}': {}", topic.title(), e.getMessage(), e);
            return topic; // Return original topic if enhancement fails
        }
    }

    private String getBestDisplayNameFromSlackMessage(SlackMessage msg) {
        if (msg == null) {
            return UNKNOWN_USER;
        }

        if (msg.getDisplayName() != null && !msg.getDisplayName().trim().isEmpty()) {
            return msg.getDisplayName();
        }

        if (msg.getUsername() != null && !msg.getUsername().trim().isEmpty()) {
            return msg.getUsername();
        }

        // Use uniqueUserId as fallback
        if (msg.getUniqueUserId() != null && !msg.getUniqueUserId().trim().isEmpty()) {
            return msg.getUniqueUserId();
        }

        return UNKNOWN_USER;
    }

    private String getBestDisplayNameFromUser(User user, String fallback) {
        if (user == null) {
            return fallback != null ? fallback : UNKNOWN_USER;
        }

        // Try to use reflection to access fields since getter methods may have compilation issues
        try {
            String displayName = getFieldValue(user, "displayName");
            if (displayName != null && !displayName.trim().isEmpty()) {
                return displayName;
            }

            String displayNameNormalized = getFieldValue(user, "displayNameNormalized");
            if (displayNameNormalized != null && !displayNameNormalized.trim().isEmpty()) {
                return displayNameNormalized;
            }

            String realNameNormalized = getFieldValue(user, "realNameNormalized");
            if (realNameNormalized != null && !realNameNormalized.trim().isEmpty()) {
                return realNameNormalized;
            }

            String name = getFieldValue(user, "name");
            if (name != null && !name.trim().isEmpty()) {
                return name;
            }
        } catch (Exception e) {
            logger.debug("Error accessing user fields: {}", e.getMessage());
        }

        return fallback != null ? fallback : UNKNOWN_USER;
    }

    private String getImageFromUser(User user) {
        if (user == null) {
            return null;
        }

        try {
            String image72 = getFieldValue(user, "image72");
            if (image72 != null && !image72.trim().isEmpty()) {
                return image72;
            }

            String imageOriginal = getFieldValue(user, "imageOriginal");
            if (imageOriginal != null && !imageOriginal.trim().isEmpty()) {
                return imageOriginal;
            }
        } catch (Exception e) {
            logger.debug("Error accessing user image fields: {}", e.getMessage());
        }

        return null;
    }

    private String getFieldValue(User user, String fieldName) {
        try {
            java.lang.reflect.Field field = user.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            Object value = field.get(user);
            return value != null ? value.toString() : null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Enhance people involved using AI-generated data with full user details
     */
    /**
     * ✅ PERFORMANCE FIX: Enhance people involved using cached user data (no DB calls)
     */
    private List<EnrichmentUserDTO> enhancePeopleInvolvedFromCache(List<EnrichmentUserDTO> aiPeopleInvolved,
                                                                     List<SlackMessage> messages,
                                                                     Map<String, EnrichmentUserDTO> cachedUsers) {
        // If AI has already provided people involved, enhance those with cached user data
        if (aiPeopleInvolved != null && !aiPeopleInvolved.isEmpty()) {
            return aiPeopleInvolved.stream()
                .map(aiUser -> cachedUsers.getOrDefault(aiUser.id(), aiUser))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        }

        // Fallback: extract from messages using cache
        return messages.stream()
            .map(msg -> msg.getUniqueUserId() != null ? msg.getUniqueUserId() : msg.getUsername())
            .filter(Objects::nonNull)
            .distinct()
            .map(userId -> cachedUsers.getOrDefault(userId, new EnrichmentUserDTO(userId, userId, userId, null)))
            .filter(user -> user.id() != null && !user.id().equals(NOT_AVAILABLE))
            .collect(Collectors.toList());
    }

    /**
     * ✅ PERFORMANCE FIX: Enhance summary per person using cached user data (no DB calls)
     */
    private List<SummaryPerPerson> enhanceSummaryPerPersonFromCache(List<SummaryPerPerson> aiSummaryPerPerson,
                                                                      List<SlackMessage> messages,
                                                                      Map<String, EnrichmentUserDTO> cachedUsers) {
        // If AI has already provided summary per person, enhance those with cached user data
        if (aiSummaryPerPerson != null && !aiSummaryPerPerson.isEmpty()) {
            return aiSummaryPerPerson.stream()
                .map(aiSummary -> enhanceSummaryWithCachedUserData(aiSummary, messages, cachedUsers))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        }

        // Fallback: extract from messages using cache
        Map<String, List<SlackMessage>> messagesByUser = messages.stream()
            .filter(msg -> msg.getUniqueUserId() != null)
            .collect(Collectors.groupingBy(SlackMessage::getUniqueUserId));

        return messagesByUser.entrySet().stream()
            .map(entry -> {
                String userId = entry.getKey();
                List<SlackMessage> userMessages = entry.getValue();

                int messageCount = userMessages.size();
                LocalDateTime firstMessageDate = userMessages.stream()
                    .map(SlackMessage::getTimestamp)
                    .filter(Objects::nonNull)
                    .min(LocalDateTime::compareTo)
                    .orElse(null);

                LocalDateTime lastMessageDate = userMessages.stream()
                    .map(SlackMessage::getTimestamp)
                    .filter(Objects::nonNull)
                    .max(LocalDateTime::compareTo)
                    .orElse(null);

                EnrichmentUserDTO userDTO = cachedUsers.getOrDefault(userId,
                    new EnrichmentUserDTO(userId, userId, userId, null));

                List<SourceDTO> sources = extractUserSourcesFromCache(userId, messages, cachedUsers);

                return new SummaryPerPerson(
                    userId,
                    userDTO.username(),
                    userDTO.displayName(),
                    userDTO.imageUrl(),
                    DEFAULT_SUMMARY_FOR_PERSON,
                    messageCount,
                    firstMessageDate,
                    lastMessageDate,
                    List.of(),
                    List.of(),
                    sources
                );
            })
            .collect(Collectors.toList());
    }

    /**
     * ✅ PERFORMANCE FIX: Enhance AI-generated summary with cached user data (no DB calls)
     */
    private SummaryPerPerson enhanceSummaryWithCachedUserData(SummaryPerPerson aiSummary,
                                                                List<SlackMessage> messages,
                                                                Map<String, EnrichmentUserDTO> cachedUsers) {
        if (aiSummary == null || aiSummary.id() == null) {
            return aiSummary;
        }

        try {
            String userId = aiSummary.id();

            // Get enhanced user from cache
            EnrichmentUserDTO enhancedUser = cachedUsers.getOrDefault(userId,
                new EnrichmentUserDTO(userId, aiSummary.username(), aiSummary.displayName(), aiSummary.imageUrl()));

            // Calculate message statistics from actual messages
            List<SlackMessage> userMessages = messages.stream()
                .filter(msg -> userId.equals(msg.getUniqueUserId()))
                .collect(Collectors.toList());

            int messageCount = userMessages.size();
            LocalDateTime firstMessageDate = userMessages.stream()
                .map(SlackMessage::getTimestamp)
                .filter(Objects::nonNull)
                .min(LocalDateTime::compareTo)
                .orElse(aiSummary.firstMessageDate());

            LocalDateTime lastMessageDate = aiSummary.lastMessageDate();
            if (lastMessageDate == null) {
                lastMessageDate = userMessages.stream()
                    .map(SlackMessage::getTimestamp)
                    .filter(Objects::nonNull)
                    .max(LocalDateTime::compareTo)
                    .orElse(null);
            }

            List<SourceDTO> enhancedSources = extractUserSourcesFromCache(userId, messages, cachedUsers);

            return new SummaryPerPerson(
                userId,
                enhancedUser.username(),
                enhancedUser.displayName(),
                enhancedUser.imageUrl(),
                aiSummary.summary(),
                Math.max(messageCount, aiSummary.messageCount()),
                firstMessageDate,
                lastMessageDate,
                aiSummary.keyContributions(),
                aiSummary.actionItems(),
                enhancedSources.isEmpty() ? aiSummary.sources() : enhancedSources
            );

        } catch (Exception e) {
            logger.warn("Failed to enhance summary for user {}: {}", aiSummary.id(), e.getMessage());
            return aiSummary;
        }
    }

    /**
     * ✅ PERFORMANCE FIX: Extract user sources using cached data (no DB calls)
     */
    private List<SourceDTO> extractUserSourcesFromCache(String userId, List<SlackMessage> messages, Map<String, EnrichmentUserDTO> cachedUsers) {
        return messages.stream()
            .filter(msg -> userId.equals(msg.getUniqueUserId()))
            .map(msg -> {
                String permalink = getPermalinkWithFallback(msg);
                String shortText = createShortTextFromMessageWithCache(msg, cachedUsers);
                String sourceName = SourceName.normalize(msg.getSource());
                return new SourceDTO(permalink, shortText, sourceName);
            })
            .distinct()
            .collect(Collectors.toList());
    }

    /**
     * ✅ PERFORMANCE FIX: Create short text from message using cached data (no DB calls)
     */
    private String createShortTextFromMessageWithCache(SlackMessage msg, Map<String, EnrichmentUserDTO> cachedUsers) {
        if (msg == null) {
            return "Unknown message";
        }

        String content = msg.getText() != null ? msg.getText() : msg.getContent();

        // Resolve username from cache
        String userId = msg.getUniqueUserId();
        String username = UNKNOWN_USER;
        if (userId != null && cachedUsers.containsKey(userId)) {
            EnrichmentUserDTO userDTO = cachedUsers.get(userId);
            username = userDTO.displayName() != null ? userDTO.displayName() : userDTO.username();
        } else if (msg.getUsername() != null) {
            username = msg.getUsername();
        } else if (msg.getDisplayName() != null) {
            username = msg.getDisplayName();
        } else if (userId != null) {
            username = userId;
        }

        if (content == null || content.trim().isEmpty()) {
            return username + ": (empty message)";
        }

        // Replace user mentions using cache
        String processedContent = resolveUserMentionsWithCache(content, cachedUsers);

        String truncatedContent = processedContent.length() > 100 ?
            processedContent.substring(0, 97) + "..." : processedContent;
        truncatedContent = truncatedContent.replaceAll("\\\\s+", " ").trim();

        return username + ": " + truncatedContent;
    }

    /**
     * ✅ PERFORMANCE FIX: Resolve user mentions using cached data (no DB calls)
     */
    private String resolveUserMentionsWithCache(String content, Map<String, EnrichmentUserDTO> cachedUsers) {
        if (content == null || content.trim().isEmpty()) {
            return content;
        }

        Pattern mentionPattern = Pattern.compile("<@([UW][A-Z0-9]+)(?:\\\\|([^>]+))?>");
        Matcher matcher = mentionPattern.matcher(content);

        StringBuffer result = new StringBuffer();

        try {
            while (matcher.find()) {
                String userId = matcher.group(1);
                String existingName = matcher.group(2);

                String resolvedName = existingName != null ? existingName : userId;

                if (cachedUsers.containsKey(userId)) {
                    EnrichmentUserDTO userDTO = cachedUsers.get(userId);
                    resolvedName = userDTO.displayName() != null ? userDTO.displayName() : userDTO.username();
                }

                matcher.appendReplacement(result, "@" + Matcher.quoteReplacement(resolvedName));
            }
            matcher.appendTail(result);
            return result.toString();
        } catch (Exception e) {
            logger.warn("Error processing user mentions in content: {}", e.getMessage());
            return content;
        }
    }

    // Original methods removed as they were replaced by cached versions for performance

    private String getPermalinkWithFallback(SlackMessage msg) {
        String permalink = msg.getPermaLink();

        if (permalink == null || permalink.trim().isEmpty()) {
            if (msg.getTeamId() != null && msg.getChannelId() != null && msg.getTs() != null) {
                permalink = String.format("slack://team=%s/channel=%s/message=%s",
                                        msg.getTeamId(), msg.getChannelId(), msg.getTs());
            } else {
                permalink = "deemerge.ai";
            }
        }

        return permalink;
    }

    // Original methods removed as they were replaced by cached versions for performance

    private List<SuggestedReply> enhanceSuggestedReplies(List<SuggestedReply> suggestedReplies, String tenantId, String workspaceId) {
        if (suggestedReplies == null || suggestedReplies.isEmpty()) {
            return List.of();
        }

        return suggestedReplies.stream()
            .map(reply -> enhanceSuggestedReply(reply, tenantId, workspaceId))
            .collect(Collectors.toList());
    }

    private SuggestedReply enhanceSuggestedReply(SuggestedReply reply, String tenantId, String workspaceId) {
        try {
            String channelId = reply.channelId();
            String channelName = reply.channelName();

            // Try to resolve channel name if missing
            if ((channelName == null || channelName.trim().isEmpty()) &&
                channelId != null && !channelId.trim().isEmpty()) {
                channelName = resolveChannelNameFromId(channelId);
            }

            return new SuggestedReply(
                reply.tone(),
                reply.replyMethod(),
                reply.recipientHandle(),
                channelName,
                channelId,
                reply.threadId(),
                reply.to(),
                reply.cc(),
                reply.subject(),
                reply.messageBody()
            );
        } catch (Exception e) {
            logger.warn("Failed to enhance suggested reply: {}", e.getMessage());
            return reply;
        }
    }

    private String resolveChannelNameFromId(String channelId) {
        try {
            return channelService.findByChannelId(channelId.trim())
                .map(channel -> channel.getChannelName())
                .orElse(channelId);
        } catch (Exception e) {
            logger.warn("Error resolving channel name for channelId {}: {}", channelId, e.getMessage());
            return channelId;
        }
    }

    @Override
    public String getStepName() {
        return "TopicEnrichment";
    }

    @Override
    public boolean continueOnFailure() {
        return true; // Continue even if topic enrichment fails
    }

    @Override
    public int getExecutionOrder() {
        return 50; // Execute after conversation enrichment but before response building
    }
}
