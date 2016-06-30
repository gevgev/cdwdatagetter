package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func formatDefaultDate() string {
	year, month, day := time.Now().Date()

	return fmt.Sprintf("%4d%02d%02d", year, int(month), day)
}

const (
	version = "0.1"
)

var (
	regionName string
	bucketName string
	date       string
	verbose    bool
	appName    string
)

func init() {

	flagRegion := flag.String("r", "us-east-1", "`AWS Region`")
	flagBucket := flag.String("b", "rovi-cdw", "`Bucket name`")
	flagDate := flag.String("d", formatDefaultDate(), "`Date`")
	flagVerbose := flag.Bool("v", true, "`Verbose`: outputs to the screen")

	flag.Parse()
	if flag.Parsed() {
		regionName = *flagRegion
		bucketName = *flagBucket
		date = *flagDate
		verbose = *flagVerbose
		appName = os.Args[0]
	} else {
		usage()
	}

}

func usage() {
	fmt.Printf("%s, ver. %s\n", appName, version)
	fmt.Println("Command line:")
	fmt.Printf("\tprompt$>%s -r <aws_region> -b <s3_bucket_name> -d <date> \n", appName)
	flag.Usage()
	os.Exit(-1)
}

func PrintParams() {
	log.Printf("Provided: -r: %s, -b: %s, -d: %v, -v: %v\n",
		regionName,
		bucketName,
		date,
		verbose,
	)

}

func main() {

	if verbose {
		PrintParams()
	}
	session := session.New(&aws.Config{
		Region: aws.String(regionName),
	})

	svc := s3.New(session)

	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	}

	resp, err := svc.ListObjects(params)
	if err != nil {
		log.Println("Failed to list objects: ", err)
		os.Exit(-1)
	}

	log.Println("Buckets:")
	for _, key := range resp.Contents {
		log.Printf(*key.Key)
	}

	filesList := filterObjectsByDate()

	for file := range filesList {
		log.Println("Downloading: ", file)
	}
}

func filterObjectsByDate() []string {
	return []string{}
}
