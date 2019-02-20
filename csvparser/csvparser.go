package csvparser

import (
	"context"
	"strconv"
	"strings"
)

// CSVOptions consists of the parser options available
// ArrayDelimiter is the delimiter for array type column names. Default value is "."
// IndexPos is the position of the index in array type column names. This can't be at the end or starting of the column name. Default value is 1
// Ex:
//		company-0-name is a valid column name but company-name-0 is not
//		In the case of `company-0-name`, the arrayDelimiter will be `-` & indexPos will be `1`
type CSVOptions struct {
	ArrayDelimiter string
	IndexPos       int
}

// CSV is the interface the for csv parser
type CSV interface {
	ToMap(ctx context.Context, csvData []map[string]string) ([]map[string]interface{}, error)
}

type csv struct {
	options CSVOptions
}

// ToMap parses CSV into a map
func (c *csv) ToMap(ctx context.Context, csvData []map[string]string) ([]map[string]interface{}, error) {

	for _, record := range csvData {

		// Cleanup quotes in the record values
		for k, v := range record {
			record[k] = strings.Replace(v, "\"", "", -1)
		}
	}

	recordStructure, err := c.getCSVStructure(ctx, csvData[0])

	// Create the map
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

// NewCSV is the initialization method for the csv parser
func NewCSV(options CSVOptions) CSV {
	if options.ArrayDelimiter == "" {
		options.ArrayDelimiter = "."
	}
	if options.IndexPos == 0 {
		options.IndexPos = 1
	}
	return &csv{
		options: options,
	}
}
