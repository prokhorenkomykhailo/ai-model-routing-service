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
            
            // Enhance each topic with detailed information
            List<TopicEnrichment> enrichedTopics = existingEnrichment.topics().stream()
                .map(topic -> enhanceTopicWithUserData(topic, messages, context))
                .collect(Collectors.toList());
            
            // Create enhanced conversation enrichment
            ConversationEnrichment enhancedEnrichment = new ConversationEnrichment(
                enrichedTopics,
                existingEnrichment.participants(),
                existingEnrichment.messages(),
                existingEnrichment.metadata()
            );
            
            context.setConversationEnrichment(enhancedEnrichment);
            
            logger.debug("Topic enrichment completed successfully. Enhanced {} topics", enrichedTopics.size());
            return PipelineStepResult.success("Topic enrichment completed successfully");
            
        } catch (Exception e) {
            logger.error("Error during topic enrichment: {}", e.getMessage(), e);
            return PipelineStepResult.success("Topic enrichment failed but continuing: " + e.getMessage());
        }
    }
    
    private TopicEnrichment enhanceTopicWithUserData(TopicEnrichment topic, List<SlackMessage> messages, PostProcessingContext context) {
        try {
            String tenantId = context.getTenantId();
            String workspaceId = messages.isEmpty() ? null : messages.get(0).getWorkspaceId();
            
            // Build user info map
            Map<String, EnrichmentUserDTO> userInfos = buildUserInfoMap(messages, tenantId, workspaceId);
            
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
            
            // Extract enhanced people involved - use AI data if available, fallback to message analysis
            List<EnrichmentUserDTO> enhancedPeopleInvolved = enhancePeopleInvolved(topic.peopleInvolved(), messages, tenantId, workspaceId);
            
            // Extract enhanced summary per person - use AI data if available, fallback to message analysis
            List<SummaryPerPerson> enhancedSummaryPerPerson = enhanceSummaryPerPerson(topic.summaryPerPerson(), messages, tenantId, workspaceId);
            
            // Extract enhanced suggested replies
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
    
    private Map<String, EnrichmentUserDTO> buildUserInfoMap(List<SlackMessage> messages, String tenantId, String workspaceId) {
        Map<String, EnrichmentUserDTO> userInfos = messages.stream()
            .filter(msg -> {
                String key = msg.getSlackUserId() != null ? msg.getSlackUserId() : msg.getUsername();
                return key != null && !key.trim().isEmpty();
            })
            .collect(Collectors.toMap(
                msg -> msg.getSlackUserId() != null ? msg.getSlackUserId() : msg.getUsername(),
                msg -> new EnrichmentUserDTO(
                    msg.getSlackUserId(), 
                    msg.getUsername(), 
                    getBestDisplayNameFromSlackMessage(msg), 
                    msg.getImage72()
                ),
                (existing, replacement) -> existing // Keep existing if duplicate
            ));
        
        updateUserInformation(userInfos, tenantId, workspaceId);
        return userInfos;
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
        
        if (msg.getSlackUserId() != null && !msg.getSlackUserId().trim().isEmpty()) {
            return msg.getSlackUserId();
        }
        
        return UNKNOWN_USER;
    }
    
    private void updateUserInformation(Map<String, EnrichmentUserDTO> userInfos, String tenantId, String workspaceId) {
        if (tenantId == null || workspaceId == null) {
            logger.warn("Cannot update user info: tenantId={}, workspaceId={}", tenantId, workspaceId);
            return;
        }
        
        userInfos.forEach((slackId, user) -> {
            try {
                if (slackId != null && !slackId.trim().isEmpty()) {
                    Optional<User> updatedUser = userService.getUser(tenantId, workspaceId, slackId);
                    if (updatedUser.isPresent()) {
                        User dbUser = updatedUser.get();
                        // Use field access since getter methods might have compilation issues
                        EnrichmentUserDTO updatedUserDTO = new EnrichmentUserDTO(
                            slackId, // Use slackId as identifier instead of dbUser.getId()
                            getBestDisplayNameFromUser(dbUser, slackId),
                            getBestDisplayNameFromUser(dbUser, slackId),
                            getImageFromUser(dbUser)
                        );
                        userInfos.put(slackId, updatedUserDTO);
                    }
                }
            } catch (Exception e) {
                logger.warn("Failed to update user info for slackId {}: {}", slackId, e.getMessage());
            }
        });
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
    private List<EnrichmentUserDTO> enhancePeopleInvolved(List<EnrichmentUserDTO> aiPeopleInvolved, 
                                                         List<SlackMessage> messages, 
                                                         String tenantId, 
                                                         String workspaceId) {
        // If AI has already provided people involved, enhance those with fresh user data
        if (aiPeopleInvolved != null && !aiPeopleInvolved.isEmpty()) {
            return aiPeopleInvolved.stream()
                .map(aiUser -> enhanceUserWithLatestData(aiUser, tenantId, workspaceId))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        }
        
        // Fallback: extract from messages if AI data is not available
        return extractPeopleInvolvedFromMessages(messages, tenantId, workspaceId);
    }
    
    /**
     * Enhance summary per person using AI-generated data with additional user details and metadata
     */
    private List<SummaryPerPerson> enhanceSummaryPerPerson(List<SummaryPerPerson> aiSummaryPerPerson,
                                                          List<SlackMessage> messages,
                                                          String tenantId,
                                                          String workspaceId) {
        // If AI has already provided summary per person, enhance those with user data and message metadata
        if (aiSummaryPerPerson != null && !aiSummaryPerPerson.isEmpty()) {
            return aiSummaryPerPerson.stream()
                .map(aiSummary -> enhanceSummaryWithUserData(aiSummary, messages, tenantId, workspaceId))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        }
        
        // Fallback: extract from messages if AI data is not available
        return extractSummaryPerPersonFromMessages(messages, tenantId, workspaceId);
    }
    
    /**
     * Enhance a user DTO with latest user data from the database
     */
    private EnrichmentUserDTO enhanceUserWithLatestData(EnrichmentUserDTO aiUser, String tenantId, String workspaceId) {
        if (aiUser == null || aiUser.id() == null) {
            return aiUser;
        }
        
        try {
            if (tenantId != null && workspaceId != null) {
                Optional<User> userOptional = userService.getUser(tenantId, workspaceId, aiUser.id());
                if (userOptional.isPresent()) {
                    User dbUser = userOptional.get();
                    String enhancedDisplayName = getBestDisplayNameFromUser(dbUser, aiUser.displayName());
                    String enhancedUsername = getFieldValue(dbUser, "name");
                    if (enhancedUsername == null || enhancedUsername.trim().isEmpty()) {
                        enhancedUsername = aiUser.username() != null ? aiUser.username() : aiUser.id();
                    }
                    String enhancedImageUrl = getImageFromUser(dbUser);
                    if (enhancedImageUrl == null) {
                        enhancedImageUrl = aiUser.imageUrl(); // Keep AI image if no DB image
                    }
                    
                    return new EnrichmentUserDTO(
                        aiUser.id(),
                        enhancedUsername,
                        enhancedDisplayName,
                        enhancedImageUrl
                    );
                }
            }
            return aiUser; // Return original if enhancement fails
        } catch (Exception e) {
            logger.warn("Failed to enhance user {} with latest data: {}", aiUser.id(), e.getMessage());
            return aiUser;
        }
    }
    
    /**
     * Enhance AI-generated summary per person with user data and message metadata
     */
    private SummaryPerPerson enhanceSummaryWithUserData(SummaryPerPerson aiSummary,
                                                       List<SlackMessage> messages,
                                                       String tenantId,
                                                       String workspaceId) {
        if (aiSummary == null || aiSummary.id() == null) {
            return aiSummary;
        }
        
        try {
            String userId = aiSummary.id();
            
            // Enhance user details
            EnrichmentUserDTO enhancedUser = enhanceUserWithLatestData(
                new EnrichmentUserDTO(userId, aiSummary.username(), aiSummary.displayName(), aiSummary.imageUrl()),
                tenantId,
                workspaceId
            );
            
            // Calculate message statistics from actual messages
            List<SlackMessage> userMessages = messages.stream()
                .filter(msg -> userId.equals(msg.getUserId()) || userId.equals(msg.getSlackUserId()))
                .collect(Collectors.toList());
            
            int messageCount = userMessages.size();
            LocalDateTime firstMessageDate = userMessages.stream()
                .map(SlackMessage::getTimestamp)
                .filter(Objects::nonNull)
                .min(LocalDateTime::compareTo)
                .orElse(aiSummary.firstMessageDate());
            
            LocalDateTime lastMessageDate = userMessages.stream()
                .map(SlackMessage::getTimestamp)
                .filter(Objects::nonNull)
                .max(LocalDateTime::compareTo)
                .orElse(aiSummary.lastMessageDate());
            
            // Extract sources from user's messages
            List<SourceDTO> enhancedSources = extractUserSources(userId, messages, tenantId, workspaceId);
            
            // Keep AI-generated summary and contributions, enhance with user data and message stats
            return new SummaryPerPerson(
                userId,
                enhancedUser.username(),
                enhancedUser.displayName(),
                enhancedUser.imageUrl(),
                aiSummary.summary(), // Keep AI-generated summary
                Math.max(messageCount, aiSummary.messageCount()), // Use higher count
                firstMessageDate,
                lastMessageDate,
                aiSummary.keyContributions(), // Keep AI-generated contributions
                aiSummary.actionItems(), // Keep AI-generated action items
                enhancedSources.isEmpty() ? aiSummary.sources() : enhancedSources // Use enhanced sources if available
            );
            
        } catch (Exception e) {
            logger.warn("Failed to enhance summary for user {}: {}", aiSummary.id(), e.getMessage());
            return aiSummary;
        }
    }
    
    /**
     * Fallback method: extract people involved from messages (renamed from original method)
     */
    private List<EnrichmentUserDTO> extractPeopleInvolvedFromMessages(List<SlackMessage> messages, String tenantId, String workspaceId) {
        return messages.stream()
            .map(msg -> msg.getSlackUserId() != null ? msg.getSlackUserId() : msg.getUsername())
            .filter(Objects::nonNull)
            .distinct()
            .map(userId -> enrichUserDTO(userId, tenantId, workspaceId))
            .filter(user -> user.id() != null && !user.id().equals(NOT_AVAILABLE))
            .collect(Collectors.toList());
    }
    
    private EnrichmentUserDTO enrichUserDTO(String userId, String tenantId, String workspaceId) {
        try {
            if (tenantId != null && workspaceId != null && userId != null && !userId.trim().isEmpty()) {
                Optional<User> userOptional = userService.getUser(tenantId, workspaceId, userId);
                if (userOptional.isPresent()) {
                    User user = userOptional.get();
                    String displayName = getBestDisplayNameFromUser(user, userId);
                    String userName = getFieldValue(user, "name");
                    if (userName == null || userName.trim().isEmpty()) {
                        userName = userId;
                    }
                    
                    return new EnrichmentUserDTO(
                        userId,
                        userName,
                        displayName,
                        getImageFromUser(user)
                    );
                }
            }
            return new EnrichmentUserDTO(userId, userId, userId, null);
        } catch (Exception e) {
            logger.warn("Failed to enrich UserDTO for user {}: {}", userId, e.getMessage());
            return new EnrichmentUserDTO(userId, userId, userId, null);
        }
    }
    
    /**
     * Fallback method: extract summary per person from messages (renamed from original method)
     */
    private List<SummaryPerPerson> extractSummaryPerPersonFromMessages(List<SlackMessage> messages, String tenantId, String workspaceId) {
        Map<String, List<SlackMessage>> messagesByUser = messages.stream()
            .filter(msg -> msg.getUserId() != null)
            .collect(Collectors.groupingBy(SlackMessage::getUserId));
        
        return messagesByUser.entrySet().stream()
            .map(entry -> {
                String userId = entry.getKey();
                List<SlackMessage> userMessages = entry.getValue();
                
                // Calculate message statistics
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
                
                // Get user details
                EnrichmentUserDTO userDTO = enrichUserDTO(userId, tenantId, workspaceId);
                
                // Extract sources from user's messages
                List<SourceDTO> sources = extractUserSources(userId, messages, tenantId, workspaceId);
                
                return new SummaryPerPerson(
                    userId,
                    userDTO.username(),
                    userDTO.displayName(),
                    userDTO.imageUrl(),
                    DEFAULT_SUMMARY_FOR_PERSON, // Could be enhanced with AI-generated summaries
                    messageCount,
                    firstMessageDate,
                    lastMessageDate,
                    List.of(), // keyContributions
                    List.of(), // actionItems
                    sources
                );
            })
            .collect(Collectors.toList());
    }
    
    private List<SourceDTO> extractUserSources(String userId, List<SlackMessage> messages, String tenantId, String workspaceId) {
        return messages.stream()
            .filter(msg -> userId.equals(msg.getUserId()))
            .map(msg -> {
                String permalink = getPermalinkWithFallback(msg);
                String shortText = createShortTextFromMessage(msg, tenantId, workspaceId);
                return new SourceDTO(permalink, shortText, msg.getSource());
            })
            .distinct()
            .collect(Collectors.toList());
    }
    
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
    
    private String createShortTextFromMessage(SlackMessage msg, String tenantId, String workspaceId) {
        if (msg == null) {
            return "Unknown message";
        }
        
        String content = msg.getText() != null ? msg.getText() : msg.getContent();
        
        // Resolve username for the message author
        String username = resolveUsernameFromMessage(msg, tenantId, workspaceId);
        
        if (content == null || content.trim().isEmpty()) {
            return username + ": (empty message)";
        }
        
        // Replace any user mentions in the content with resolved usernames
        String processedContent = resolveUserMentionsInContent(content, tenantId, workspaceId);
        
        String truncatedContent = processedContent.length() > 100 ? 
            processedContent.substring(0, 97) + "..." : processedContent;
        truncatedContent = truncatedContent.replaceAll("\\s+", " ").trim();
        
        return username + ": " + truncatedContent;
    }
    
    private String resolveUsernameFromMessage(SlackMessage msg, String tenantId, String workspaceId) {
        // First try to get the userId/slackUserId
        String userId = msg.getSlackUserId() != null ? msg.getSlackUserId() : msg.getUserId();
        
        if (userId != null && !userId.trim().isEmpty() && tenantId != null && workspaceId != null) {
            try {
                Optional<User> userOptional = userService.getUser(tenantId, workspaceId, userId);
                if (userOptional.isPresent()) {
                    User user = userOptional.get();
                    return getBestDisplayNameFromUser(user, userId);
                }
            } catch (Exception e) {
                logger.debug("Failed to resolve username for userId {}: {}", userId, e.getMessage());
            }
        }
        
        // Fall back to available username fields from the message
        if (msg.getUsername() != null && !msg.getUsername().trim().isEmpty()) {
            return msg.getUsername();
        }
        
        if (msg.getDisplayName() != null && !msg.getDisplayName().trim().isEmpty()) {
            return msg.getDisplayName();
        }
        
        if (userId != null && !userId.trim().isEmpty()) {
            return userId;
        }
        
        return UNKNOWN_USER;
    }
    
    private String resolveUserMentionsInContent(String content, String tenantId, String workspaceId) {
        if (content == null || content.trim().isEmpty() || tenantId == null || workspaceId == null) {
            return content;
        }
        
        // Pattern to match Slack user mentions like <@U1234567890> or <@U1234567890|username>
        Pattern mentionPattern = Pattern.compile("<@([UW][A-Z0-9]+)(?:\\|([^>]+))?>");
        Matcher matcher = mentionPattern.matcher(content);
        
        StringBuffer result = new StringBuffer();
        
        try {
            while (matcher.find()) {
                String userId = matcher.group(1);
                String existingName = matcher.group(2); // The part after |, if present
                
                String resolvedName = existingName != null ? existingName : userId;
                
                try {
                    Optional<User> userOptional = userService.getUser(tenantId, workspaceId, userId);
                    if (userOptional.isPresent()) {
                        User user = userOptional.get();
                        resolvedName = getBestDisplayNameFromUser(user, resolvedName);
                    }
                } catch (Exception e) {
                    logger.debug("Failed to resolve user mention for userId {}: {}", userId, e.getMessage());
                }
                
                matcher.appendReplacement(result, "@" + Matcher.quoteReplacement(resolvedName));
            }
            matcher.appendTail(result);
            return result.toString();
        } catch (Exception e) {
            logger.warn("Error processing user mentions in content: {}", e.getMessage());
            return content; // Return original content if processing fails
        }
    }
    
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
