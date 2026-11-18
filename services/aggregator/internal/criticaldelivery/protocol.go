// Package criticaldelivery reconciles immutable critical notifications with
// signed receipts from the designated Field Team receiver.
package criticaldelivery

import (
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

const MaxPayload = storage.MaxCriticalPayloadBytes

func ReceiptMessage(r storage.CriticalReceipt) ([]byte, error) {
	for _, value := range []string{r.ReceiverID, r.EventID, r.HazardID, r.PayloadSHA256} {
		if len(value) == 0 || len(value) > 128 || strings.ContainsAny(value, "\r\n") {
			return nil, errors.New("invalid receipt identity")
		}
	}
	hash, err := hex.DecodeString(r.PayloadSHA256)
	if err != nil || len(hash) != sha256.Size || r.ReceivedAt.IsZero() {
		return nil, errors.New("invalid receipt hash/time")
	}
	return []byte("BNPB-CRITICAL-RECEIPT-V1\n" + r.ReceiverID + "\n" + r.EventID + "\n" + r.HazardID + "\n" + r.PayloadSHA256 + "\n" + r.ReceivedAt.UTC().Format(time.RFC3339Nano) + "\n"), nil
}

func SignReceipt(key ed25519.PrivateKey, r storage.CriticalReceipt) (storage.CriticalReceipt, error) {
	message, err := ReceiptMessage(r)
	if err != nil {
		return r, err
	}
	if len(key) != ed25519.PrivateKeySize {
		return r, errors.New("invalid receiver private key")
	}
	r.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(key, message))
	return r, nil
}
func VerifyReceipt(key ed25519.PublicKey, receiver string, r storage.CriticalReceipt) error {
	message, err := ReceiptMessage(r)
	if err != nil {
		return err
	}
	signature, err := base64.StdEncoding.DecodeString(r.Signature)
	if err != nil || r.ReceiverID != receiver || len(key) != ed25519.PublicKeySize || !ed25519.Verify(key, message, signature) {
		return errors.New("invalid receiver signature")
	}
	return nil
}
func RequestMAC(secret string, body []byte) string {
	h := hmac.New(sha256.New, []byte(secret))
	_, _ = h.Write(body)
	return "sha256=" + hex.EncodeToString(h.Sum(nil))
}
func ValidRequestMAC(secret string, body []byte, signature string) bool {
	return len(secret) >= 16 && hmac.Equal([]byte(RequestMAC(secret, body)), []byte(signature))
}
