package core

type Task interface {
	Handle() error
}

type TaskHandlerFunc func() error

func (f TaskHandlerFunc) Handle() error {
	return f()
}

type TaskTypeManagerInterface interface {
	RegisterType(typ Task)
	ExistType(typ string) bool
	ExtractType(typ string) (task Task, err error)
}
