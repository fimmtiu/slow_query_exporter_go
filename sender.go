package main

import (
	"encoding/json"
	"fmt"
	"github.com/fatih/structs"
	"github.com/robertkowalski/graylog-golang"
	"os"
	"unicode"
)

type Sender struct {
	gelf     *gelf.Gelf
	hostname string
	input    chan SlowQuery
}

func NewSender(conf Config) (Sender, error) {
	gelfConfig := gelf.Config{
		GraylogPort:     conf.GraylogPort,
		GraylogHostname: conf.GraylogHost,
	}
	hostname, err := os.Hostname()
	if err != nil {
		return Sender{}, err
	}

	return Sender{gelf.New(gelfConfig), hostname, make(chan SlowQuery)}, nil
}

func (sender *Sender) ListenForQueries() {
	for {
		query := <-sender.input

		// We have to convert the CamelCase struct fields to snake_case for Graylog.
		queryMap := structs.Map(query)
		jsonMap := make(map[string]interface{})
		for key, value := range queryMap {
			jsonMap[convertKeyToGraylogFormat(key)] = value
		}

		// Set some standard GELF fields. (See http://docs.graylog.org/en/2.0/pages/gelf.html)
		jsonMap["version"] = "1.1"
		jsonMap["host"] = sender.hostname
		jsonMap["timestamp"] = query.Time.Unix()
		jsonMap["short_message"] = fmt.Sprintf("Slow query on %s: %.2f seconds", query.Host, query.QueryTime)

		json_query, _ := json.Marshal(jsonMap)
		fmt.Printf("Query: %s\n\n", json_query)
		sender.gelf.Log(string(json_query))
	}
}

func (sender *Sender) SendQuery(query SlowQuery) {
	sender.input <- query
}

// Graylog's field names are in snake case with a leading underscore.
func convertKeyToGraylogFormat(s string) string {
	runes := []rune(s)
	result := "_"

	for i := 0; i < len(runes); i++ {
		if unicode.IsUpper(runes[i]) {
			if i > 0 {
				result += "_"
			}
			result += string(unicode.ToLower(runes[i]))
		} else {
			result += string(runes[i])
		}
	}

	return result
}
