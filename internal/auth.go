	package main

	import (
		"fmt"
		"os"
		"sync"
		"time"

		"github.com/golang-jwt/jwt/v5"
	)

	type RateLimiter struct {
		mu       sync.Mutex
		buckets  map[int]*bucket
		rate     int
		interval time.Duration
	}

	type bucket struct {
		tokens   int
		lastSeen time.Time
	}

	func NewRateLimiter(rate int, interval time.Duration) *RateLimiter {
		rl := &RateLimiter{
			buckets:  make(map[int]*bucket),
			rate:     rate,
			interval: interval,
		}
		go rl.cleanup()
		return rl
	}

	func (rl *RateLimiter) Allow(userID int) bool {
		rl.mu.Lock()
		defer rl.mu.Unlock()

		now := time.Now()
		b, exists := rl.buckets[userID]
		if !exists {
			b = &bucket{tokens: rl.rate, lastSeen: now}
			rl.buckets[userID] = b
		}

		elapsed := now.Sub(b.lastSeen)
		refill := int(elapsed / rl.interval)
		if refill > 0 {
			b.tokens += refill
			if b.tokens > rl.rate {
				b.tokens = rl.rate
			}
			b.lastSeen = now
		}

		if b.tokens <= 0 {
			return false
		}

		b.tokens--
		return true
	}

	func (rl *RateLimiter) cleanup() {
		ticker := time.NewTicker(2 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			rl.mu.Lock()
			threshold := time.Now().Add(-5 * time.Minute)
			for uid, b := range rl.buckets {
				if b.lastSeen.Before(threshold) {
					delete(rl.buckets, uid)
				}
			}
			rl.mu.Unlock()
		}
	}

	type ConnLimiter struct {
		mu      sync.Mutex
		buckets map[int]*connBucket
		limit   int
	}

	type connBucket struct {
		count  int
		window time.Time
	}

	func NewConnLimiter(limit int) *ConnLimiter {
		cl := &ConnLimiter{
			buckets: make(map[int]*connBucket),
			limit:   limit,
		}
		go cl.cleanup()
		return cl
	}

	func (cl *ConnLimiter) Allow(userID int) bool {
		cl.mu.Lock()
		defer cl.mu.Unlock()

		now := time.Now()
		b, exists := cl.buckets[userID]
		if !exists || now.Sub(b.window) >= time.Minute {
			b = &connBucket{count: 0, window: now}
			cl.buckets[userID] = b
		}

		if b.count >= cl.limit {
			return false
		}
		b.count++
		return true
	}

	func (cl *ConnLimiter) cleanup() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			cl.mu.Lock()
			threshold := time.Now().Add(-2 * time.Minute)
			for uid, b := range cl.buckets {
				if b.window.Before(threshold) {
					delete(cl.buckets, uid)
				}
			}
			cl.mu.Unlock()
		}
	}

	func ValidateJWT(tokenString string) (int, error) {
		jwtSecret := os.Getenv("JWT_SECRET")
		if jwtSecret == "" {
			return 0, fmt.Errorf("JWT_SECRET environment variable is not set")
		}

		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return []byte(jwtSecret), nil
		},
			jwt.WithExpirationRequired(),
			jwt.WithLeeway(30*time.Second),
		)
		if err != nil {
			return 0, fmt.Errorf("invalid token: %w", err)
		}

		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok || !token.Valid {
			return 0, fmt.Errorf("invalid token claims")
		}

		if sub, ok := claims["sub"].(float64); ok && sub > 0 {
			return int(sub), nil
		}

		if subStr, ok := claims["sub"].(string); ok && subStr != "" {
			var id int
			if _, err := fmt.Sscanf(subStr, "%d", &id); err == nil && id > 0 {
				return id, nil
			}
		}

		if uid, ok := claims["user_id"].(float64); ok && uid > 0 {
			return int(uid), nil
		}

		return 0, fmt.Errorf("missing or invalid user identity in token")
	}
