package pkg

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// getBuckets return all s3 buckets from specified region
func getBuckets(client s3iface.S3API, region string) *s3.ListBucketsOutput {
	input := &s3.ListBucketsInput{}

	result, err := client.ListBuckets(input)
	if err != nil {
		log.Fatal("Not able to get list of S3 buckets ", err)
	}

	return result
}

// ParseS3Tags parse output from getBuckets and return instances id and specified tags.
func ParseS3Tags(tagsToRead string, client s3iface.S3API, region string, session *session.Session) [][]string {
	s3Output := getBuckets(client, region)
	rows := addHeadersToCsv(tagsToRead, "Name")
	for _, bucket := range s3Output.Buckets {
		// bucket.Name

		// bucket := "my-bucket"
		bucketRegion, err := s3manager.GetBucketRegion(context.Background(), session, *bucket.Name, region)

		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
				fmt.Fprintf(os.Stderr, "unable to find bucket %s's region not found\n", bucket)
			}
			break
		}
		if bucketRegion == region {

			s3Tags, err := client.GetBucketTagging(&s3.GetBucketTaggingInput{Bucket: bucket.Name})
			if err != nil {

				if err.(awserr.Error).Code() == "NoSuchTagSet" {
					fmt.Println("Tag set for bucket", *bucket.Name, "doesn't exist")
				} else if err.(awserr.Error).Code() == "AuthorizationHeaderMalformed" {
					fmt.Println("Bucket ", *bucket.Name, "is not in your region", "region")
				} else {
					fmt.Println("Not able to get tags ", err)
				}
			}

			tags := map[string]string{}
			for _, tag := range s3Tags.TagSet {
				tags[*tag.Key] = *tag.Value
			}
			rows = addTagsToCsv(tagsToRead, tags, rows, *bucket.Name)
		}
	}
	return rows
}

// TagS3 tag instances. Take as input data from csv file. Where first column id
func TagS3(csvData [][]string, client s3iface.S3API) {

	for r := 1; r < len(csvData); r++ {

		bucket := csvData[r][0]
		s3Tags, _ := client.GetBucketTagging(&s3.GetBucketTaggingInput{Bucket: &bucket})

		for c := 1; c < len(csvData[0]); c++ {
			tagAlreadyExists := false
			for p, tag := range s3Tags.TagSet {
				if *tag.Key == csvData[0][c] {
					tagAlreadyExists = true
					s3Tags.TagSet[p].Value = &csvData[r][c]

					break
				}
			}
			if !tagAlreadyExists {
				s3Tags.TagSet = append(s3Tags.TagSet, &s3.Tag{
					Key:   &csvData[0][c],
					Value: &csvData[r][c],
				})
			}
		}

		input := &s3.PutBucketTaggingInput{
			Bucket: aws.String(csvData[r][0]),
			Tagging: &s3.Tagging{
				TagSet: s3Tags.TagSet,
			},
		}

		_, err := client.PutBucketTagging(input)
		if awsErrorHandle(err) {
			return
		}
	}
}
