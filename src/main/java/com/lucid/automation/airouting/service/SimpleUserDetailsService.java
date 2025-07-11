package com.lucid.automation.airouting.service;

import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;

import java.util.Collections;

/**
 * Simple UserDetailsService implementation for AI Routing Service.
 * This service creates a basic UserDetails object for JWT validation purposes.
 * The actual user authentication is handled by the Auth Service.
 */
@Service
public class SimpleUserDetailsService implements UserDetailsService {

    @Override
    public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {
        // For AI routing service, we just need a basic UserDetails object for JWT validation
        // The actual user authentication and role validation happens at the gateway level
        return User.builder()
                .username(username)
                .password("") // Password not needed for JWT validation
                .authorities(Collections.singletonList(new SimpleGrantedAuthority("ROLE_USER")))
                .build();
    }
}
