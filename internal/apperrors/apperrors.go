package apperrors

import "fmt"

type Kind string

const (
	KindValidation Kind = "validation"
	KindNotFound   Kind = "not_found"
	KindInternal   Kind = "internal"
)

type Error struct {
	Kind    Kind
	Message string
}

func (e *Error) Error() string {
	return e.Message
}

func Validation(format string, args ...any) *Error {
	return &Error{
		Kind:    KindValidation,
		Message: fmt.Sprintf(format, args...),
	}
}

func NotFound(format string, args ...any) *Error {
	return &Error{
		Kind:    KindNotFound,
		Message: fmt.Sprintf(format, args...),
	}
}
