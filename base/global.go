package base

import (
	"log"
	"os"
)

var (
	GlobalLogger  *log.Logger
	GlobalLogFile *os.File
)
