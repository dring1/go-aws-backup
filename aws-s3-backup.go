package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"

	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
)

const FileChunk = 5242880
const MultiThreshold = 104857600

func main() {

	// 1. auth
	// 2. @param file/dir to move to s3
	// 3. @param file/dir in s3 to move to
	auth, err := aws.EnvAuth()
	if err != nil {
		log.Fatal(err)
	}

	client := s3.New(auth, aws.USWest2)

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
	files := FileList(src)

	var wg sync.WaitGroup
	completed := make(chan string)

	wg.Add(len(files))
	for _, val := range files {
		go func(src string) {
			defer wg.Done()
			log.Println(src)
			err = Backup(src, dest, client)
			completed <- src
			if err != nil {
				log.Fatal(err)
			}
		}(val)
	}

	go func() {
		for res := range completed {
			fmt.Printf("Done uploading: %s\n", res)
		}
	}()
	wg.Wait()

}

func Backup(src string, dest string, client *s3.S3) error {

	file, err := os.Open(src)

	if err != nil {
		return err
	}

	defer file.Close()

	fileInfo, _ := file.Stat()

	var fileSize int64 = fileInfo.Size()
	if fileSize > MultiThreshold {
		return BigUpload(src, dest, client)
	}

	bytes := make([]byte, fileSize)

	buffer := bufio.NewReader(file)
	_, err = buffer.Read(bytes)

	if err != nil {
		return err
	}

	fileType := http.DetectContentType(bytes)
	bucket := client.Bucket(dest)
	err = bucket.Put(src, bytes, fileType, s3.ACL("private"))
	if err != nil {
		return err
	}

	return nil
}

func BigUpload(src string, dest string, client *s3.S3) error {
	file, err := os.Open(src)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	r := bufio.NewReader(file)

	bucket := client.Bucket(dest)
	multi, err := bucket.InitMulti(dest, "application/octet-stream", s3.ACL("private"))

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	var buf = new(bytes.Buffer)
	var readBytes = make([]byte, FileChunk)
	parts := []s3.Part{}
	for i := 1; ; i++ {
		n, err := r.Read(readBytes)
		if err != nil && err != io.EOF {
			fmt.Println(err)
			os.Exit(1)
		}
		if n != 0 {
			buf.Write(readBytes[0:n])
		}
		if n == 0 {
			break
		}
		log.Printf("Chunk size %d part %d", n, i)
		part, err := multi.PutPart(i, bytes.NewReader(buf.Bytes()))
		if err != nil {
			log.Fatal(err)
		}
		parts = append(parts, part)

	}

	err = multi.Complete(parts)
	if err != nil {
		return err
	}

	return nil

}

func FileList(src string) []string {
	list := []string{}
	filepath.Walk(src, func(path string, f os.FileInfo, err error) error {
		if !f.IsDir() {
			list = append(list, path)
		}
		return nil
	})
	return list
}
