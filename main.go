
package main
 
import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
 
	chat "chat-service/internal"
)
 
func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[nexora] ")
	log.Println("starting Nexora Chat Service...")
 
	if err := validateEnv(); err != nil {
		log.Fatalf("config error: %v", err)
	}
 
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
 
	db, err := chat.NewDB(ctx)
	if err != nil {
		log.Fatalf("database: %v", err)
	}
	defer db.Close()
 
	hub := chat.NewHub(db)
	go hub.Run()
 
	limiter := chat.NewRateLimiter(30, time.Second)
	connLimiter := chat.NewConnLimiter(10)
	server := chat.NewServer(hub, db, limiter, connLimiter)
 
	port := envOr("PORT", "8080")
	httpServer := &http.Server{
		Addr:         ":" + port,
		Handler:      corsMiddleware(server.Mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
 
	go func() {
		log.Printf("listening on :%s", port)
		if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server: %v", err)
		}
	}()
 
	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	log.Printf("signal received: %v, shutting down...", sig)
 
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()
 
	if err := httpServer.Shutdown(shutCtx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	log.Println("stopped")
}
 
func validateEnv() error {
	required := []string{"DATABASE_URL", "JWT_SECRET"}
	for _, k := range required {
		if os.Getenv(k) == "" {
			return fmt.Errorf("required env var %q is not set", k)
		}
	}
	return nil
}
 
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
 
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Client-Info, Apikey")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
