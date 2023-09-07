package utils

import (
	"bufio"
	"fmt"
	"os"
)

type StringReader struct {
	Scanner   *bufio.Scanner
	sizeBatch int
}

func (S *StringReader) ReadBatch() []string {
	var readed int
	result := []string{}
	for S.Scanner.Scan() {
		result = append(result, S.Scanner.Text())
		readed++
		if S.sizeBatch == readed {
			break
		}
	}
	if err := S.Scanner.Err(); err != nil {
		return []string{}
	}
	return result
}

func (S *StringReader) ReadOne() string {
	if S.Scanner.Scan() {
		return S.Scanner.Text()
	}
	return ""
}

func NewStringReader(filepath string, sizeBatch int) (*StringReader, error) {
	SReader := StringReader{}
	if isExist, _ := PathExists(filepath); !isExist {
		return &SReader, fmt.Errorf("file %v not exists or access error", filepath)
	}
	file, err := os.Open(filepath)
	if err != nil {
		return &SReader, err
	}
	SReader.Scanner = bufio.NewScanner(file)
	SReader.sizeBatch = sizeBatch
	return &SReader, nil
}
