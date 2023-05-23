package s3

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3 struct {
	client     *s3.Client
	bucketName string
}

func NewS3(endpointURL string, bucket string) (*S3, error) {
	endpointResolver := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID && endpointURL != "" {
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           endpointURL,
					SigningRegion: region,
				}, nil
			}
			// returning EndpointNotFoundError will allow the service to fallback to it's default resolution
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithDefaultRegion("eu-west-1"),
		config.WithEndpointResolverWithOptions(endpointResolver),
	)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(
		cfg,
		func(o *s3.Options) {
			o.UsePathStyle = true
		},
	)
	return &S3{
		client:     client,
		bucketName: bucket,
	}, nil
}

func (c *S3) ReaderFromS3Location(ctx context.Context, objectName string) (io.ReadCloser, error) {
	resp, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucketName),
		Key:    aws.String(objectName),
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}
