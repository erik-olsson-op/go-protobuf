package main

import (
	"encoding/csv"
	"fmt"
	"github.com/erik-olsson-op/go-protobuf/model"
	"google.golang.org/protobuf/proto"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func main() {
	fmt.Println("get authors...")
	authors := getAuthors()
	fmt.Println("get books...")
	books := getBooks(authors)

	fmt.Println("writes proto messages to disk...")
	err := writeProtoToDisk(books)
	if err != nil {
		panic(err)
	}

	fmt.Println("read proto messages from disk...")
	for i := 1; i <= 10; i++ {
		var data, _ = os.ReadFile(fmt.Sprintf("data/book-%d.binpb", i))

		b := model.Book{}
		err := proto.Unmarshal(data, &b)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Id: %d - Title: %v - Author: %v - Category: %v\n", b.Id, b.Title, b.Author, b.Category)
		time.Sleep(time.Second)
	}
}

func readCsvFile(csvFilePath string) [][]string {
	fd, err := os.Open(csvFilePath)
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	fileReader := csv.NewReader(fd)
	records, err := fileReader.ReadAll()
	if err != nil {
		panic(err)
	}
	return records
}

// getAuthors - read csv file and creates author protobuf message
func getAuthors() []model.Author {
	authorCsvRows := readCsvFile("data/author.csv")
	var authors = make([]model.Author, 0)
	for _, rows := range authorCsvRows {
		id, _ := strconv.ParseInt(rows[0], 10, 64)
		name := rows[1]

		var author = model.Author{
			Id:   id,
			Name: name,
		}
		authors = append(authors, author)
	}
	return authors
}

// getBooks - read csv file and creates book protobuf message
func getBooks(authors []model.Author) []model.Book {
	bookCsvRows := readCsvFile("data/book.csv")
	var books = make([]model.Book, 0)
	for _, rows := range bookCsvRows {
		id, _ := strconv.ParseInt(rows[0], 10, 64)
		title := rows[1]
		a := authors[rand.Intn(5)]
		c := getRandomCategory(rand.Intn(3))
		var book = model.Book{
			Id:    id,
			Title: title,
			Author: []*model.Author{
				{
					Id:   a.GetId(),
					Name: a.GetName(),
				},
			},
			Category: c,
		}
		books = append(books, book)
	}
	return books
}

func getRandomCategory(rn int) model.Category {
	switch rn {
	case 1:
		return model.Category_Novel
	case 2:
		return model.Category_SciFi
	case 3:
		return model.Category_Fantasy
	default:
		return model.Category_Novel
	}
}

func writeProtoToDisk(books []model.Book) error {
	for _, book := range books {
		data, err := proto.Marshal(&book)
		if err != nil {
			return err
		}
		err = os.WriteFile(fmt.Sprintf("data/book-%d.binpb", book.GetId()), data, 0777)
		if err != nil {
			return err
		}
	}
	fmt.Println("done!")
	return nil
}
