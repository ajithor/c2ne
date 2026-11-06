package crypto

import (
    "crypto/aes"
    "crypto/cipher"
    "crypto/rand"
    //"encoding/binary"
    "errors"
    "io"
    "fmt"
)

func Encrypt(key, plaintext []byte) (string, error) {
    block, err := aes.NewCipher(key)
    if err != nil {
        fmt.Println("aesNewCipher err -", err)
        return "", err
    }
    gcm, err := cipher.NewGCM(block)
    if err != nil {
        fmt.Println("newGcm err -", err)
        return "", err
    }

    nonce := make([]byte, gcm.NonceSize()) // 12 bytes
    if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
        fmt.Println("Nonce Read err -", err)
        return "", err
    }

    // Seal(dst, nonce, plaintext, additionalData)
    // prepends nonce to the output so you get: nonce || ciphertext || tag
    return string(gcm.Seal(nonce, nonce, plaintext, nil)), nil
}

func Decrypt(key, ciphertext []byte) (string, error) {
    block, err := aes.NewCipher(key)
    if err != nil {
        fmt.Println("aesNewCipher 2 err -", err)
        return "", err
    }
    gcm, err := cipher.NewGCM(block)
    if err != nil {
        fmt.Println("newGcm2 err -", err)
        return "", err
    }

    nonceSize := gcm.NonceSize()
    if len(ciphertext) < nonceSize+gcm.Overhead() {
        fmt.Println("Detected ciphertext too small")
        return "", errors.New("ciphertext too short")
    }

    nonce, ct := ciphertext[:nonceSize], ciphertext[nonceSize:]
    pt, err := gcm.Open(nil, nonce, ct, nil)
    return string(pt), err
}
