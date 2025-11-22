package main

import (
	"os"
)


func main() {
	if len(os.Args) < 2 {
		panic("Please provide 'producer' or 'consumer' as an argument")
	}
	
	switch os.Args[1] {
	case "producer":
		producer()
	case "consumer":
		consumer()
	default:
		panic("Unknown argument. Use 'producer' or 'consumer'")
	}
}