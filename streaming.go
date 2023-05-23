package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"

	"github.com/TheEdgeOfRage/streaming-poc/constants"
	"github.com/TheEdgeOfRage/streaming-poc/s3"
)

type Metadata struct {
	QueryID     int      `json:"query_id"`
	ColumnNames []string `json:"column_names"`
}

type Req struct {
	ExecutionID string    `json:"execution_id"`
	Metadata    Metadata  `json:"metadata"`
	Z           int       `json:"z"`
	Rows        io.Reader `json:"-"`
}

type ReqBatch struct {
	ExecutionID string   `json:"execution_id"`
	Metadata    Metadata `json:"metadata"`
	Z           int      `json:"z"`
	Rows        []Row    `json:"rows"`
}

type Row map[string]any

func GetData(ctx context.Context, s3 *s3.S3) (io.Reader, error) {
	s3Reader, err := s3.ReaderFromS3Location(ctx, "data.json.zst")
	if err != nil {
		return nil, fmt.Errorf("failed to get S3 reader: %w", err)
	}
	reader, err := getZSTReader(s3Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to get ZST reader: %w", err)
	}

	return reader, nil
}

func SerializeReqToJson(req Req, writer io.Writer) error {
	buf, err := json.Marshal(req)
	if err != nil {
		return err
	}

	_, err = writer.Write(buf[:(len(buf) - 1)])
	if err != nil {
		return err
	}
	_, err = writer.Write([]byte(",\"rows\":"))
	if err != nil {
		return err
	}
	_, err = io.Copy(writer, req.Rows)
	if err != nil {
		return err
	}
	_, err = writer.Write([]byte("}"))
	if err != nil {
		return err
	}

	return nil
}

func getZSTReader(stream io.Reader) (io.Reader, error) {
	return zstd.NewReader(
		stream,
		zstd.WithDecoderConcurrency(0),
		zstd.WithDecoderMaxMemory(512*1024*1024),
	)
}

func decodeJSON(rows []Row, stream io.Reader) error {
	var err error

	dec := json.NewDecoder(stream)
	// read open bracket
	_, err = dec.Token()
	if err != nil {
		return fmt.Errorf("unexpected JSON token: %w", err)
	}

	// decode while the array contains values
	i := 0
	for dec.More() {
		var row Row
		err = dec.Decode(&row)
		if err != nil {
			return fmt.Errorf("failed to decode row: %w", err)
		}

		rows[i] = row
		i += 1
	}

	// read closing bracket
	_, err = dec.Token()
	if err != nil {
		return fmt.Errorf("unexpected JSON token: %w", err)
	}

	return nil
}

func ParseStreaming(reader io.Reader, c *gin.Context) error {
	req := Req{
		ExecutionID: "123",
		Metadata: Metadata{
			QueryID:     1,
			ColumnNames: []string{"a", "b"},
		},
		Z:    1,
		Rows: reader,
	}

	// serialize req to stdout
	c.Writer.WriteHeader(200)
	return SerializeReqToJson(req, c.Writer)
}

func ParseBatch(reader io.Reader, c *gin.Context) error {
	rows := make([]Row, constants.RowCount)
	err := decodeJSON(rows, reader)
	if err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}
	req := ReqBatch{
		ExecutionID: "123",
		Metadata: Metadata{
			QueryID:     1,
			ColumnNames: []string{"a", "b"},
		},
		Z:    1,
		Rows: rows,
	}

	// serialize req to stdout
	buf, err := json.Marshal(req)
	if err != nil {
		return err
	}
	c.Writer.WriteHeader(200)
	_, err = c.Writer.Write(buf)
	return err
}

func main() {
	//setup gin server
	endpointURL := os.Getenv("S3_ENDPOINT_URL") // don't set env var to use AWS S3
	bucketName := os.Getenv("S3_BUCKET_NAME")
	if bucketName == "" {
		log.Fatal("S3_BUCKET_NAME env var is not set")
	}
	s3, err := s3.NewS3(
		endpointURL,
		bucketName,
	)
	if err != nil {
		log.Fatal("failed to create S3 client: ", err)
	}

	r := gin.Default()
	r.GET("/streaming", func(c *gin.Context) {
		reader, err := GetData(c.Request.Context(), s3)
		if err != nil {
			log.Fatal("failed to get data: ", err)
		}
		err = ParseStreaming(reader, c)
		if err != nil {
			log.Fatal("failed to serialize json: ", err)
		}
		var memstats runtime.MemStats
		runtime.ReadMemStats(&memstats)
		log.Print("sys: ", memstats.Sys/1024/1024, "MB")
	})
	r.GET("/batch", func(c *gin.Context) {
		reader, err := GetData(c.Request.Context(), s3)
		if err != nil {
			log.Fatal("failed to get data: ", err)
			c.AbortWithStatus(500)
			return
		}
		err = ParseBatch(reader, c)
		if err != nil {
			log.Fatal("failed to serialize json: ", err)
			c.AbortWithStatus(500)
			return
		}
		var memstats runtime.MemStats
		runtime.ReadMemStats(&memstats)
		log.Print("sys: ", memstats.Sys/1024/1024, "MB")
	})
	r.Run()

}
