package logger

import "log"

const (
	reset  = "\033[0m"
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
	cyan   = "\033[36m"
)

func Debug(format string, args ...any) {
	log.Printf(cyan+"[DEBUG] "+format+reset, args...)
}

func Info(format string, args ...any) {
	log.Printf(green+"[INFO] "+format+reset, args...)
}

func Warn(format string, args ...any) {
	log.Printf(yellow+"[WARN] "+format+reset, args...)
}

func Error(format string, args ...any) {
	log.Printf(red+"[ERROR] "+format+reset, args...)
}
