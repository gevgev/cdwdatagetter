package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func formatDefaultDate() string {
	year, month, day := time.Now().Date()

	return fmt.Sprintf("%4d%02d%02d", year, int(month), day)
}

const (
	version     = "0.1"
	MAXATTEMPTS = 3
)

var (
	regionName      string
	bucketName      string
	date            string
	msoListFilename string
	prefixPath      string
	maxAttempts     int

	verbose bool
	appName string
)

func init() {

	flagRegion := flag.String("r", "us-east-1", "`AWS Region`")
	flagBucket := flag.String("b", "rovi-cdw", "`Bucket name`")
	flagDate := flag.String("d", formatDefaultDate(), "`Date`")
	flagMsoFileName := flag.String("m", "mso-list.csv", "Filename for `MSO` list")
	flagPrefixPath := flag.String("p", "event/tv_viewership", "`Prefix path` in the bucket")
	flagMaxAttempts := flag.Int("M", MAXATTEMPTS, "`Max attempts` to retry download from aws.s3")

	flagVerbose := flag.Bool("v", true, "`Verbose`: outputs to the screen")

	flag.Parse()
	if flag.Parsed() {
		regionName = *flagRegion
		bucketName = *flagBucket
		date = *flagDate
		msoListFilename = *flagMsoFileName
		prefixPath = *flagPrefixPath
		maxAttempts = *flagMaxAttempts

		verbose = *flagVerbose
		appName = os.Args[0]
	} else {
		usage()
	}

}

func usage() {
	fmt.Printf("%s, ver. %s\n", appName, version)
	fmt.Println("Command line:")
	fmt.Printf("\tprompt$>%s -r <aws_region> -b <s3_bucket_name> -p <bucket_key_path> -d <date> -m <mso-list-file-name> -M <max_retry>\n", appName)
	flag.Usage()
	os.Exit(-1)
}

func PrintParams() {
	log.Printf("Provided: -r: %s, -b: %s, -d: %v, -m %s, -p %s, -M %d, -v: %v\n",
		regionName,
		bucketName,
		date,
		msoListFilename,
		prefixPath,
		maxAttempts,
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

var failedFilesChan chan string
var downloadedReportChannel chan bool

func main() {
	startTime := time.Now()
	downloaded := 0
	failedFilesChan = make(chan string)
	downloadedReportChannel = make(chan bool)

	failedFilesList := []string{}
	var wg sync.WaitGroup

	// Listening to failed reports
	go func() {
		for {
			key, more := <-failedFilesChan
			if more {
				failedFilesList = append(failedFilesList, key)
			} else {
				return
			}
		}
	}()

	// listening to succeeded reports
	go func() {
		for {
			_, more := <-downloadedReportChannel
			if more {
				downloaded++
			} else {
				return
			}
		}
	}()

	if verbose {
		PrintParams()
	}

	session := session.New(&aws.Config{
		Region: aws.String(regionName),
	})

	svc := s3.New(session)

	msoList := getMsoNamesList()

	for _, mso := range msoList {
		prefix := formatPrefix(prefixPath, mso.Code)
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
				wg.Add(1)
				go processSingleDownload(*key.Key, &wg)
			}
		}

	}

	if verbose {
		log.Println("All files sent to be downloaded. Waiting for completetion...")
	}
	wg.Wait()
	if verbose {
		log.Println("All download jobs completed, closing failed/succeeded jobs channel")
	}
	close(failedFilesChan)
	close(downloadedReportChannel)
	ReportFailedFiles(failedFilesList)
	log.Printf("Processed %d MSO's, %d files, in %v\n", len(msoList), downloaded, time.Since(startTime))
}

func ReportFailedFiles(failedFilesList []string) {
	if len(failedFilesList) > 0 {
		for _, key := range failedFilesList {
			log.Println("Failed downloading: ", key)
		}
	} else {
		log.Println("No failed downloads")
	}
}

func processSingleDownload(key string, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < maxAttempts; i++ {
		log.Println("Downloading: ", key)
		if downloadFile(key) {
			if verbose {
				log.Println("Successfully downloaded: ", key)
			}
			downloadedReportChannel <- true
			return
		} else {
			if verbose {
				log.Println("Failed, going to sleep for: ", key)
			}
			time.Sleep(time.Duration(10) * time.Second)
		}
	}
	failedFilesChan <- key
}

func createPath(path string) error {
	err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
	return err
}

func downloadFile(filename string) bool {

	err := createPath(filename)
	if err != nil {
		log.Println("Could not create folder: ", filepath.Dir(filename))
		return false
	}

	file, err := os.Create(filename)
	if err != nil {
		log.Println("Failed to create file: ", err)
		return false
	}

	defer file.Close()

	downloader := s3manager.NewDownloader(session.New(&aws.Config{Region: aws.String(regionName)}))

	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(filename),
		})

	if err != nil {
		log.Printf("Failed to download file: %s, Error: %s ", filename, err)
		return false
	}

	log.Println("Downloaded file ", file.Name(), numBytes, " bytes")
	return true
}
