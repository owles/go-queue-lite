package go_queue_lite

import (
	"github.com/owles/go-queue-lite/core"
	"reflect"
	"strings"
	"sync"
)

type TaskTypeManager struct {
	types sync.Map
}

func NewTaskTypeManager() *TaskTypeManager {
	return &TaskTypeManager{}
}

func (p *TaskTypeManager) ExistType(typ string) bool {
	_, ok := p.types.Load(typ)
	return ok
}

func (p *TaskTypeManager) RegisterType(typ core.Task) {
	dataType := reflect.TypeOf(typ)
	p.types.LoadOrStore(dataType.String(), dataType)
}

func (p *TaskTypeManager) ExtractType(typ string) (task core.Task, err error) {
	if strings.HasPrefix(typ, "*") {
		if taskType, ok := p.types.Load(typ); ok {
			ref := reflect.New(taskType.(reflect.Type).Elem()).Interface()
			if task, ok = ref.(core.Task); ok {
				return task, nil
			}
		}
	} else {
		if taskType, ok := p.types.Load(typ); ok {
			ref := reflect.New(taskType.(reflect.Type)).Interface()
			if task, ok = ref.(core.Task); ok {
				return task, nil
			}
		}
	}

	return core.TaskHandlerFunc(func() error {
		return core.ErrTypeExtract
	}), core.ErrTypeExtract
}
