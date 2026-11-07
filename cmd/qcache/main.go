package main

import (
	"fmt"
	"qcache/internal/storage"
)

func main() {

	cache := storage.New()

	err := cache.Set("user:1", []byte("Piyush"))

	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	value, exists := cache.Get("user:1")

	if exists {
		fmt.Println("Cached value:", string(value))
	}

	fmt.Println("Total entries:", cache.Len())
}
