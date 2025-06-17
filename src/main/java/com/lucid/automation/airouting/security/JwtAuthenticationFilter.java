package com.lucid.automation.airouting.security;

import com.lucid.automation.airouting.service.JwtService;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.security.SignatureException;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.lang.NonNull;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.web.authentication.WebAuthenticationDetailsSource;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.Set;

/**
 * JWT Authentication Filter for AI Routing Service.
 * This filter validates JWT tokens for incoming requests and sets up the security context.
 */
@Component
public class JwtAuthenticationFilter extends OncePerRequestFilter {

    private static final Logger logger = LoggerFactory.getLogger(JwtAuthenticationFilter.class);
    
    private final JwtService jwtService;
    private final UserDetailsService userDetailsService;
    
    private static final Set<String> PUBLIC_PATH_PREFIXES = Set.of(
        "/actuator", "/swagger-ui", "/v3/api-docs", "/swagger-ui.html", "/api/messages","/api/", "ai/"
    );

    public JwtAuthenticationFilter(JwtService jwtService, UserDetailsService userDetailsService) {
        this.jwtService = jwtService;
        this.userDetailsService = userDetailsService;
    }

    @Override
    protected void doFilterInternal(
            @NonNull HttpServletRequest request,
            @NonNull HttpServletResponse response,
            @NonNull FilterChain filterChain) throws ServletException, IOException {

        try {
            String path = request.getRequestURI();
            
            // Skip filter for public paths
            if (isPublicPath(path)) {
                filterChain.doFilter(request, response);
                return;
            }

            final String authHeader = request.getHeader("Authorization");
            
            // Check for missing or invalid Authorization header
            if (authHeader == null || !authHeader.startsWith("Bearer ")) {
                handleUnauthorized(response, "Missing or invalid Authorization header");
                return;
            }

            final String jwt = authHeader.substring(7);
            
            try {
                String username = jwtService.extractUsername(jwt);
                
                if (username == null) {
                    handleUnauthorized(response, "Invalid token: username not found");
                    return;
                }
                
                if (SecurityContextHolder.getContext().getAuthentication() == null) {
                    UserDetails userDetails = userDetailsService.loadUserByUsername(username);
                    
                    if (jwtService.isTokenValid(jwt, userDetails)) {
                        // Create enhanced user details with tenant information
                        CustomUserDetails customUserDetails = createCustomUserDetails(jwt, userDetails);
                        
                        UsernamePasswordAuthenticationToken authToken = new UsernamePasswordAuthenticationToken(
                            customUserDetails,
                            null,
                            customUserDetails.getAuthorities()
                        );
                        authToken.setDetails(new WebAuthenticationDetailsSource().buildDetails(request));
                        SecurityContextHolder.getContext().setAuthentication(authToken);
                        
                        // Add tenant information to request headers for downstream processing
                        addTenantInfoToRequest(request, jwt);
                    } else {
                        handleUnauthorized(response, "Invalid or expired token");
                        return;
                    }
                }
            } catch (ExpiredJwtException e) {
                logger.warn("JWT token expired: {}", e.getMessage());
                handleUnauthorized(response, "Token expired");
                return;
            } catch (SignatureException e) {
                logger.warn("Invalid JWT signature: {}", e.getMessage());
                handleUnauthorized(response, "Invalid token signature");
                return;
            } catch (MalformedJwtException e) {
                logger.warn("Malformed JWT token: {}", e.getMessage());
                handleUnauthorized(response, "Malformed token");
                return;
            } catch (Exception e) {
                logger.error("JWT processing error: {}", e.getMessage(), e);
                handleUnauthorized(response, "Token processing error");
                return;
            }

            filterChain.doFilter(request, response);
            
        } catch (Exception e) {
            logger.error("Authentication filter error: {}", e.getMessage(), e);
            handleUnauthorized(response, "Authentication failed");
        }
    }

    /**
     * Creates custom user details with tenant information extracted from JWT.
     */
    private CustomUserDetails createCustomUserDetails(String jwt, UserDetails userDetails) {
        String tenantId = jwtService.extractTenantId(jwt);
        String tenantSchema = jwtService.extractTenantSchema(jwt);
        String role = jwtService.extractRole(jwt);
        String userId = jwtService.extractUserId(jwt);
        
        return new CustomUserDetails(
            userDetails.getUsername(),
            userDetails.getPassword(),
            userDetails.getAuthorities(),
            tenantId,
            tenantSchema,
            role,
            userId,
            userDetails.isEnabled()
        );
    }

    /**
     * Adds tenant information to request attributes for downstream access.
     */
    private void addTenantInfoToRequest(HttpServletRequest request, String jwt) {
        String tenantId = jwtService.extractTenantId(jwt);
        String tenantSchema = jwtService.extractTenantSchema(jwt);
        String userId = jwtService.extractUserId(jwt);
        
        if (tenantId != null) {
            request.setAttribute("tenantId", tenantId);
        }
        if (tenantSchema != null) {
            request.setAttribute("tenantSchema", tenantSchema);
        }
        if (userId != null) {
            request.setAttribute("userId", userId);
        }
    }

    /**
     * Checks if the request path is a public path that doesn't require authentication.
     */
    private boolean isPublicPath(String path) {
        return PUBLIC_PATH_PREFIXES.stream().anyMatch(path::startsWith);
    }

    /**
     * Handles unauthorized requests by sending a 401 response.
     */
    private void handleUnauthorized(HttpServletResponse response, String message) throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.getWriter().write(String.format("{\"error\":\"Unauthorized\",\"message\":\"%s\"}", message));
    }
}
