package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/TheEdgeOfRage/streaming-poc/constants"
)

type Data struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
	City string `json:"city"`
}

func RandStringRunes(n int) string {
	// Generate a random string of length n
	// Used to generate random names and cities
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		// Generate a random index into the letterRunes slice
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

// Generate a json file with an array of 50 million Data structs
// The file is named "data.json" and is placed in the current directory
// Randomly generate the Name, Age, and City fields
func Generate() {
	// Create a file
	file, err := os.Create("data.json")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer file.Close()

	// Create a random number generator
	rand.Seed(time.Now().UnixNano())

	file.WriteString("[\n")
	// Generate random data and append it to the slice
	for i := 0; i < constants.RowCount; i++ {
		data, err := json.Marshal(Data{
			Name: RandStringRunes(10),
			Age:  rand.Intn(100),
			City: RandStringRunes(10),
		})
		if err != nil {
			panic(err)
		}
		file.Write(data)
		if i != constants.RowCount-1 {
			file.WriteString(",\n")
		}
	}

	file.WriteString("\n]")
}

func main() {
	Generate()
}
