package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
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
	regionName      string
	bucketName      string
	date            string
	msoListFilename string

	verbose bool
	appName string
)

func init() {

	flagRegion := flag.String("r", "us-east-1", "`AWS Region`")
	flagBucket := flag.String("b", "rovi-cdw", "`Bucket name`")
	flagDate := flag.String("d", formatDefaultDate(), "`Date`")
	flagMsoFileName := flag.String("m", "mso-list.csv", "Filename for `MSO` list")
	flagVerbose := flag.Bool("v", true, "`Verbose`: outputs to the screen")

	flag.Parse()
	if flag.Parsed() {
		regionName = *flagRegion
		bucketName = *flagBucket
		date = *flagDate
		msoListFilename = *flagMsoFileName
		verbose = *flagVerbose
		appName = os.Args[0]
	} else {
		usage()
	}

}

func usage() {
	fmt.Printf("%s, ver. %s\n", appName, version)
	fmt.Println("Command line:")
	fmt.Printf("\tprompt$>%s -r <aws_region> -b <s3_bucket_name> -d <date> -m <mso-list-file-name>\n", appName)
	flag.Usage()
	os.Exit(-1)
}

func PrintParams() {
	log.Printf("Provided: -r: %s, -b: %s, -d: %v, -m %s, -v: %v\n",
		regionName,
		bucketName,
		date,
		msoListFilename,
		verbose,
	)

}

type MsoType struct {
	Code string
	Name string
}

func getMsoNamesList() []MsoType {
	msoList := []MsoType{}

	msoFile, err := os.Open(msoListFilename)
	if err != nil {
		log.Fatalf("Could not open Mso List file: %s, Error: %s\n", msoListFilename, err)
	}

	r := csv.NewReader(msoFile)
	records, err := r.ReadAll()
	if err != nil {
		log.Fatalf("Could not read MSO file: %s, Error: %s\n", msoListFilename, err)
	}

	for _, record := range records {
		msoList = append(msoList, MsoType{record[0], record[1]})
	}
	return msoList
}

func formatPrefix(path, msoCode string) string {
	return fmt.Sprintf("%s/%s/delta/", path, msoCode)
}

func main() {

	if verbose {
		PrintParams()
	}

	session := session.New(&aws.Config{
		Region: aws.String(regionName),
	})

	svc := s3.New(session)

	msoList := getMsoNamesList()

	for _, mso := range msoList {
		prefix := formatPrefix("event/tv_viewership", mso.Code)
		if verbose {
			log.Println("Prefix: ", prefix)
		}
		params := &s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
			Prefix: aws.String(prefix),
		}

		resp, err := svc.ListObjects(params)
		if err != nil {
			log.Println("Failed to list objects: ", err)
			os.Exit(-1)
		}

		log.Println("Number of objects: ", len(resp.Contents))
		log.Println("Files:")
		for _, key := range resp.Contents {
			log.Printf(*key.Key)
			if strings.Contains(*key.Key, prefix+date) {
				log.Println("Downloading: ", *key.Key)
			}
		}

	}
}

func filterObjectsByDate(date string) []string {

	filteredFileNames := []string{}

	return filteredFileNames

}
