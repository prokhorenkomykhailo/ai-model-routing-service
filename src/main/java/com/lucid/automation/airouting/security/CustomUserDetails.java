package com.lucid.automation.airouting.security;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.Collection;

/**
 * Custom UserDetails implementation that includes tenant information
 * extracted from JWT tokens.
 */
public class CustomUserDetails implements UserDetails {
    
    private final String username;
    private final String password;
    private final Collection<? extends GrantedAuthority> authorities;
    private final String tenantId;
    private final String tenantSchema;
    private final String role;
    private final String userId;
    private final boolean enabled;

    public CustomUserDetails(String username, String password, Collection<? extends GrantedAuthority> authorities,
                           String tenantId, String tenantSchema, String role, String userId, boolean enabled) {
        this.username = username;
        this.password = password;
        this.authorities = authorities;
        this.tenantId = tenantId;
        this.tenantSchema = tenantSchema;
        this.role = role;
        this.userId = userId;
        this.enabled = enabled;
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return authorities;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public boolean isAccountNonExpired() {
        return true;
    }

    @Override
    public boolean isAccountNonLocked() {
        return true;
    }

    @Override
    public boolean isCredentialsNonExpired() {
        return true;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    // Additional getters for tenant information
    public String getTenantId() {
        return tenantId;
    }

    public String getTenantSchema() {
        return tenantSchema;
    }

    public String getRole() {
        return role;
    }

    public String getUserId() {
        return userId;
    }
}
