package reader

import (
	"bufio"
	"context"
	gocsv "encoding/csv"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// CSVOptions consists of the reader options available
// HTTPTimeout is required only if you want a custom client to handle the requests. By Default, the package keeps 10s of end-to-end request timeout with 5s TCP connect timeout & 5s of TLS handshake timeout
type CSVOptions struct {
	HTTPClient *http.Client
}

// CSV is a lightweight interface for reading csv files
type CSV interface {
	FromPath(ctx context.Context, filePath string) ([]map[string]string, error)
	FromURL(ctx context.Context, url string) ([]map[string]string, error)
}

type csv struct {
	options CSVOptions
}

func (c *csv) getRecords(ctx context.Context, csvData io.Reader) ([]map[string]string, error) {
	var lines []map[string]string

	reader := gocsv.NewReader(csvData)
	lineCount := 0
	var mapKeys []string
	var err error
	for {
		if lineCount == 0 {
			mapKeys, err = reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			lineCount++
			continue
		}

		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		record := make(map[string]string)
		for i, val := range line {
			record[mapKeys[i]] = strings.TrimSpace(val)
		}
		lines = append(lines, record)
		lineCount++
	}

	return lines, nil
}

// FromPath reads CSV from a file path
func (c *csv) FromPath(ctx context.Context, filePath string) ([]map[string]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return c.getRecords(ctx, bufio.NewReader(file))
}

// FromURL reads the CSV from a url
func (c *csv) FromURL(ctx context.Context, url string) ([]map[string]string, error) {
	resp, err := c.options.HTTPClient.Get(url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.New("Unexpected HTTP status code")
	}
	defer resp.Body.Close()

	return c.getRecords(ctx, bufio.NewReader(resp.Body))
}

// NewCSV is the initialization method for csv reader
// httpTimeout is required only in case of
func NewCSV(options CSVOptions) CSV {
	if options.HTTPClient == nil {
		var netTransport = &http.Transport{
			Dial: (&net.Dialer{
				Timeout: 5 * time.Second,
			}).Dial,
			TLSHandshakeTimeout: 5 * time.Second,
		}
		options.HTTPClient = &http.Client{
			Timeout:   time.Second * 10,
			Transport: netTransport,
		}
	}

	return &csv{
		options: options,
	}
}
