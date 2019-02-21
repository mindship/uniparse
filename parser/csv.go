package parser

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
)

// CSVOptions consists of the parser options available
// ArrayDelimiter is the delimiter for array type column names. Default value is "."
// IndexPos is the position of the index (0-indexed) in array type column names. This can't be at the end or starting of the column name. Default value is 1
// Ex:
//		company-0-name is a valid column name but company-name-0 is not
//		In the case of `company-0-name`, the arrayDelimiter will be `-` & indexPos will be `1`
// StructTag is the tag of the struct for struct mapping. Default value is `json`
type CSVOptions struct {
	ArrayDelimiter string
	IndexPos       int
	StructTag      string
}

// CSV is the interface the for csv parser
type CSV interface {
	ToMap(ctx context.Context, csvData []map[string]string) ([]map[string]interface{}, error)
	ToJSON(ctx context.Context, csvData []map[string]string) (string, error)
	ToStruct(ctx context.Context, csvData []map[string]string, res interface{}) error
}

type csv struct {
	options CSVOptions
}

// ToMap parses CSV into a map
func (c *csv) ToMap(ctx context.Context, csvData []map[string]string) ([]map[string]interface{}, error) {
	var res []map[string]interface{}

	for _, record := range csvData {

		// Cleanup quotes in the record values
		for k, v := range record {
			record[k] = strings.Replace(v, "\"", "", -1)
		}
	}

	recordStructure, err := c.getCSVStructure(ctx, csvData[0])
	if err != nil {
		return res, err
	}

	// Create the map
	for _, record := range csvData {

		recordMap, err := c.recordToMap(ctx, recordStructure, record)
		if err != nil {
			return res, err
		}
		res = append(res, recordMap)
	}

	return res, nil
}

func (c *csv) getCSVStructure(ctx context.Context, example map[string]string) (map[string][]string, error) {
	recordStructure := map[string][]string{}

	indexPos := c.options.IndexPos

	for k := range example {
		keyParts := strings.Split(k, c.options.ArrayDelimiter)

		if len(keyParts) <= indexPos {
			// It is a single valued record
			recordStructure[k] = []string{}
			continue
		}

		// Check if it is an array type record
		_, err := strconv.Atoi(keyParts[indexPos])
		if err == nil {
			// It is an array type record
			key := strings.Join(keyParts[0:indexPos], c.options.ArrayDelimiter)
			subKey := strings.Join(keyParts[indexPos+1:len(keyParts)], c.options.ArrayDelimiter)

			// Check if key exists in the record structure
			_, ok := recordStructure[key]
			if !ok {
				recordStructure[key] = []string{subKey}
			} else {
				// Append the subkey into the key's record structure if it doesn't already have it
				isSubkeyPresent := false
				for _, rec := range recordStructure[key] {
					if rec == subKey {
						isSubkeyPresent = true
						break
					}
				}
				if !isSubkeyPresent {
					recordStructure[key] = append(recordStructure[key], subKey)
				}
			}
		} else {
			recordStructure[k] = []string{}
		}
	}

	return recordStructure, nil

}

func (c *csv) recordToMap(ctx context.Context, recordStructure map[string][]string, record map[string]string) (map[string]interface{}, error) {
	recordMap := make(map[string]interface{})
	for key, subKeys := range recordStructure {
		if len(subKeys) == 0 {
			// This is a single valued key
			recordMap[key] = record[key]
			continue
		}

		// Find the length of slice for the key
		length := 0
		run := true
		subKey := subKeys[0]
		for run == true {
			recordKey := strings.Join([]string{key, strconv.Itoa(length), subKey}, c.options.ArrayDelimiter)

			_, ok := record[recordKey]
			if !ok {
				// We have reached the end index for this key & subkey combination
				run = false
			} else {
				length++
			}
		}

		// Handle array type keys
		keyData := make([]map[string]string, length)
		for _, subKey := range subKeys {

			run := true
			index := 0
			for run == true {
				recordKey := strings.Join([]string{key, strconv.Itoa(index), subKey}, c.options.ArrayDelimiter)

				val, ok := record[recordKey]
				if !ok {
					// We have reached the end index for this key & subkey combination
					run = false
					break
				}
				if len(keyData[index]) == 0 {
					keyData[index] = make(map[string]string, len(subKeys))
				}

				keyData[index][subKey] = val
				index++
			}
		}
		recordMap[key] = keyData
	}

	return recordMap, nil
}

func (c *csv) ToJSON(ctx context.Context, csvData []map[string]string) (string, error) {
	convertedToMap, err := c.ToMap(ctx, csvData)
	if err != nil {
		return "", err
	}

	convertedToJSON, err := json.Marshal(convertedToMap)
	if err != nil {
		return "", err
	}

	return string(convertedToJSON), nil
}

func (c *csv) ToStruct(ctx context.Context, csvData []map[string]string, res interface{}) error {
	convertedToMap, err := c.ToMap(ctx, csvData)
	if err != nil {
		return err
	}

	stringToDateTimeHook := func(
		f reflect.Type,
		t reflect.Type,
		data interface{}) (interface{}, error) {
		if t == reflect.TypeOf(time.Time{}) && f == reflect.TypeOf("") {
			return time.Parse(time.RFC3339, data.(string))
		}

		return data, nil
	}
	config := mapstructure.DecoderConfig{
		DecodeHook: stringToDateTimeHook,
		Result:     res,
		TagName:    c.options.StructTag,
	}

	decoder, err := mapstructure.NewDecoder(&config)
	if err != nil {
		return err
	}

	err = decoder.Decode(convertedToMap)
	if err != nil {
		return err
	}

	return nil
}

// NewCSV is the initialization method for the csv parser
func NewCSV(options CSVOptions) CSV {
	if options.ArrayDelimiter == "" {
		options.ArrayDelimiter = "."
	}
	if options.IndexPos == 0 {
		options.IndexPos = 1
	}
	if options.StructTag == "" {
		options.StructTag = "json"
	}
	return &csv{
		options: options,
	}
}
