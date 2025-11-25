package com.lucid.automation.airouting.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.lang.NonNull;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Filter that authenticates internal service requests.
 * This filter checks for X-Internal-Service and X-Service-Name headers and
 * bypasses JWT authentication for legitimate internal service calls.
 */
@Component
public class InternalServiceAuthenticationFilter extends OncePerRequestFilter {

    private static final Logger logger = LoggerFactory.getLogger(InternalServiceAuthenticationFilter.class);

    private static final String INTERNAL_SERVICE_HEADER = "X-Internal-Service";
    private static final String SERVICE_NAME_HEADER = "X-Service-Name";

    @Autowired
    private DiscoveryClient discoveryClient;

    @Override
    protected void doFilterInternal(
            @NonNull HttpServletRequest request,
            @NonNull HttpServletResponse response,
            @NonNull FilterChain filterChain) throws ServletException, IOException {

        try {
            String path = request.getRequestURI();

            // Check if this is an internal service request
            if (isInternalServiceRequest(request)) {
                String serviceName = request.getHeader(SERVICE_NAME_HEADER);

                logger.info("✅ Authenticated internal service request from '{}' to path: {}",
                           serviceName, path);

                // Create authentication for internal service
                UsernamePasswordAuthenticationToken authToken = new UsernamePasswordAuthenticationToken(
                    "internal-service-" + serviceName,
                    null,
                    List.of(new SimpleGrantedAuthority("ROLE_INTERNAL_SERVICE"))
                );

                SecurityContextHolder.getContext().setAuthentication(authToken);

                // Add service information to request attributes
                request.setAttribute("serviceName", serviceName);
                request.setAttribute("isInternalService", true);
            }

            filterChain.doFilter(request, response);

        } catch (Exception e) {
            logger.error("Error in internal service authentication filter for path: {} - {}",
                        request.getRequestURI(), e.getMessage());
            filterChain.doFilter(request, response);
        }
    }

    /**
     * Checks if the request is coming from an internal service
     */
    private boolean isInternalServiceRequest(HttpServletRequest request) {
        String serviceName = request.getHeader(SERVICE_NAME_HEADER);
        String internalServiceFlag = request.getHeader(INTERNAL_SERVICE_HEADER);

        logger.debug("=== INTERNAL SERVICE CHECK ===");
        logger.debug("Incoming X-Service-Name header: '{}'", serviceName);
        logger.debug("Incoming X-Internal-Service header: '{}'", internalServiceFlag);

        // Primary method: Check service name header against registered services
        if (serviceName != null && isRegisteredService(serviceName)) {
            logger.debug("✅ Request authorized from registered service: {}", serviceName);
            return true;
        }

        // Secondary method: Check for internal service flag with service name validation
        if ("true".equalsIgnoreCase(internalServiceFlag)) {
            // Even with internal service flag, we still need a valid service name
            if (serviceName != null && isRegisteredService(serviceName)) {
                logger.debug("✅ Request authorized with internal service flag from registered service: {}", serviceName);
                return true;
            } else {
                logger.warn("❌ Request with internal service flag but invalid/missing service name: {}", serviceName);
            }
        }

        return false;
    }

    /**
     * Checks if the service name is registered in Eureka
     */
    private boolean isRegisteredService(String serviceName) {
        try {
            Set<String> registeredServices = getRegisteredServices();
            boolean isRegistered = registeredServices.contains(serviceName.toUpperCase());

            if (isRegistered) {
                logger.debug("Service '{}' is registered in Eureka", serviceName);
            } else {
                logger.debug("Service '{}' is NOT registered in Eureka. Registered services: {}",
                    serviceName, registeredServices);
            }

            return isRegistered;
        } catch (Exception e) {
            logger.warn("Error checking if service '{}' is registered: {}", serviceName, e.getMessage());
            // In case of error, be conservative and deny access
            return false;
        }
    }

    /**
     * Gets the list of registered services from Eureka
     */
    private Set<String> getRegisteredServices() {
        if (discoveryClient == null) {
            logger.warn("DiscoveryClient is null, cannot check registered services");
            return Set.of();
        }

        List<String> services = discoveryClient.getServices();
        return services.stream()
                .map(String::toUpperCase)
                .collect(Collectors.toSet());
    }
}
