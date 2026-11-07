package main

import (
	"fmt"
	"time"

	"qcache/internal/storage"
)

func main() {

	cache := storage.New()
	defer cache.Close()

	err := cache.SetWithTTL(
		"session:123",
		[]byte("Piyush"),
		10*time.Second,
	)

	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	value, exists := cache.Get("session:123")

	if exists {
		fmt.Println("Cached value:", string(value))
	}

	fmt.Println("Total entries:", cache.Len())
}
