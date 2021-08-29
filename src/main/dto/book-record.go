package dto

import "github.com/linkedin/goavro/v2"

type Author struct {
	FirstName string
	LastName  string
}

type Book struct {
	ID     string
	Title  string
	Author Author
	Errors []string
}

func (book *Book) ToStringMap() map[string]interface{} {

	datumIn := map[string]interface{}{
		"ID":    string(book.ID),
		"Title": string(book.Title),
	}

	if len(book.Errors) > 0 {
		datumIn["Errors"] = goavro.Union("array", book.Errors)
	} else {
		datumIn["Errors"] = goavro.Union("null", nil)
	}

	authorDatum := map[string]interface{}{
		"FirstName": string(book.Author.FirstName),
		"LastName":  string(book.Author.LastName),
	}

	datumIn["Author"] = goavro.Union("my.namespace.com.author", authorDatum)

	return datumIn
}
