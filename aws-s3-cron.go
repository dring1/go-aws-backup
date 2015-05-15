package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/robfig/cron"

	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
)

type Bucket s3.Bucket

const FileChunk = 5242880

func main() {

	// 1. auth
	// 2. @param file/dir to move to s3
	// 3. @param file/dir in s3 to move to
	auth, err := aws.EnvAuth()
	if err != nil {
		log.Fatal(err)
	}

	client := s3.New(auth, aws.USEast)
	resp, err := client.ListBuckets()

	if err != nil {
		log.Fatal(err)
	}
	if len(os.Args) == 1 {
		log.Fatal(errors.New(fmt.Sprintf("Did not receive src or dest path")))
	}
	if len(os.Args) == 2 {
		log.Fatal(errors.New(fmt.Sprintf("Did not receive a dest path")))
	}

	src := os.Args[1]
	dest := os.Args[2]

	// Which bucket

	cronJob := cron.New()
	cronJob.AddFunc("@daily", func() {
		err = Backup(src, dest, client)
		if err != nil {
			log.Fatal(err)
		}
	})
	log.Print(fmt.Sprintf("%+v", cronJob))
	log.Print(fmt.Sprintf("src: %s , dest: %s ", src, dest))
	log.Print(fmt.Sprintf("%T %+v", resp.Buckets, resp.Buckets))

	bucket := resp.Buckets[1]
	log.Print(fmt.Sprintf("%T %+v", bucket, bucket))
}

// read all files of a directory - maintain structure
// store filepaths
// for each file

func Backup(src string, dest string, client *s3.S3) error {

	// file might be huge
	// upload in chunk?
	// multipart upload
	// check if bigger than 5mb or not
	file, err := os.Open(src)

	if err != nil {
		return err
	}
	defer file.Close()

	fileInfo, _ := file.Stat()
	var fileSize int64 = fileInfo.Size()

	bytes := make([]byte, fileSize)

	buffer := bufio.NewReader(file)
	_, err = buffer.Read(bytes)

	if err != nil {
		return err
	}

	fileType := http.DetectContentType(bytes)
	bucket := client.Bucket(dest)
	multi, err := bucket.InitMulti(src, fileType, s3.ACL("public-read"))

	if err != nil {
		return err
	}

	parts, err := multi.PutAll(file, FileChunk)

	if err != nil {
		return err
	}

	err = multi.Complete(parts)

	if err != nil {
		return err
	}

	fmt.Println("PutAll upload complete")

	return nil
}

func FileList(src string) []string {
	return []string{"123"}
}
