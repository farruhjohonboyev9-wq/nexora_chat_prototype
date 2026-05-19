package chat
 
import (
	"errors"
	"fmt"
	"os"
	"time"
 
	"github.com/golang-jwt/jwt/v5"
)
 
var (
	ErrMissingSecret   = errors.New("JWT_SECRET environment variable is not set")
	ErrInvalidToken    = errors.New("invalid token")
	ErrInvalidClaims   = errors.New("invalid token claims")
	ErrMissingIdentity = errors.New("missing or invalid user identity in token")
)
 
// ValidateJWT — JWT tokenni tekshirib, foydalanuvchi ID sini qaytaradi.
// Token HMAC bilan imzolangan va muddati tugamagan bo'lishi shart.
func ValidateJWT(tokenString string) (int, error) {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		return 0, ErrMissingSecret
	}
 
	token, err := jwt.Parse(tokenString,
		func(t *jwt.Token) (interface{}, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return []byte(secret), nil
		},
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(30*time.Second),
	)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
 
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return 0, ErrInvalidClaims
	}
 
	if id := extractUserID(claims); id > 0 {
		return id, nil
	}
 
	return 0, ErrMissingIdentity
}
 
// extractUserID — claimlardan foydalanuvchi ID sini olishga harakat qiladi.
// "sub" (float64 yoki string) va "user_id" maydonlarini tekshiradi.
func extractUserID(claims jwt.MapClaims) int {
	if sub, ok := claims["sub"].(float64); ok && sub > 0 {
		return int(sub)
	}
	if sub, ok := claims["sub"].(string); ok {
		var id int
		if _, err := fmt.Sscanf(sub, "%d", &id); err == nil && id > 0 {
			return id
		}
	}
	if uid, ok := claims["user_id"].(float64); ok && uid > 0 {
		return int(uid)
	}
	return 0
}
 
