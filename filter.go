// Package sqltor предоставляет возможность соединения нескольких sql-запросов в один.
// Пакет работает со структурами типа Filter, в которых передаются необходимые для выбора поля, таблицы,
// условия соединения таблиц и условия where. Для корректной работы функции CreateQuery необходимо передавать
// таблицы и условия их соединения как упорядоченное множество. Например, если в одном из запросов таблица t1
// передана раньше, чем t2, то в остальных запросах должно быть также. При невозможности соединить таблицы
// выбрасывается ошибка. Необходимо предусмтореть воможность прокидывать и возвращать аргументы, необходимые
// для передачи в функции пакетов sql.
package sqltor

import (
	"container/list"
	"fmt"
	"strings"
	"sync"
)

var f = &filters{
	m:    make(map[string]*Filter),
	lock: sync.RWMutex{},
}

type filters struct {
	m    map[string]*Filter
	lock sync.RWMutex
}

func (f *filters) get(key string) *Filter {
	f.lock.RLock()
	defer f.lock.RUnlock()

	return f.m[key]
}

func (f *filters) set(key string, value *Filter) {
	f.lock.Lock()
	f.m[key] = value
	f.lock.Unlock()
}

type Filter struct {
	SelectedColumns []string
	Tables          []string
	JoinsOn         []string
	WhereClauses    []string
}

func NewFilter() FilterCreator {
	return &Filter{}
}

func (f *Filter) Select(columns ...string) Columns {
	f.SelectedColumns = append(f.SelectedColumns, columns...)

	return f
}

func (f *Filter) From(table string) Table {
	f.Tables = append(f.Tables, table)

	return f
}

func (f *Filter) Join(table string) JoinedTable {
	f.Tables = append(f.Tables, table)

	return f
}

func (f *Filter) On(cond string) Table {
	f.JoinsOn = append(f.JoinsOn, cond)

	return f
}

func (f *Filter) Where(conds ...string) *Filter {
	f.WhereClauses = append(f.WhereClauses, conds...)

	return f
}

type FilterCreator interface {
	Select(columns ...string) Columns
}

type Columns interface {
	From(table string) Table
}

type Table interface {
	Join(table string) JoinedTable
	Where(conds ...string) *Filter
}

type JoinedTable interface {
	On(cond string) Table
}

func RegisterFilter(filterName string, filter *Filter) {
	f.set(filterName, filter)
}

func CreateQuery(filters []string, args [][]interface{}) (string, []interface{}, error) {
	if len(filters) == 0 {
		return "", nil, NoFiltersErr()
	}

	var retArgs []interface{}

	selectsSet := make(map[string]struct{})
	var selectsSlice []string

	var wheresSlice []string

	usedTables := make(map[string]*list.Element)

	filterList := list.New()

	firstFilter := f.get(filters[0])
	if firstFilter == nil {
		return "", nil, FilterDoesNotExistErr(filters[0])
	}
	if len(firstFilter.Tables) == 0 {
		return "", nil, ZeroTablesErr(filters[0])
	}
	firstTable := filterList.PushBack(firstFilter.Tables[0])
	usedTables[firstFilter.Tables[0]] = firstTable

	argInd := 0

	for _, key := range filters {
		filter := f.get(key)
		if filter == nil {
			return "", nil, FilterDoesNotExistErr(key)
		}
		if len(filter.Tables) == 0 {
			return "", nil, ZeroTablesErr(key)
		}

		err := sqlJoin(filter, filterList, usedTables)
		if err != nil {
			return "", nil, err
		}

		for _, s := range filter.SelectedColumns {
			if _, ok := selectsSet[s]; !ok {
				selectsSet[s] = struct{}{}
				selectsSlice = append(selectsSlice, s)
			}
		}

		for _, w := range filter.WhereClauses {
			if strings.Contains(w, "?") {
				if argInd >= len(args) {
					return "", nil, NotEnoughArgs(len(args), argInd+1)
				}

				a := args[argInd]

				w = strings.ReplaceAll(w, "(?", "(?"+strings.Repeat(", ?", len(a)-1))
				retArgs = append(retArgs, a...)

				argInd++
			}
			wheresSlice = append(wheresSlice, w)
		}
	}

	if argInd < len(args) {
		return "", nil, TooManyArgs(len(args), argInd)
	}

	joinStmt := ""
	for e := filterList.Front(); e != nil; e = e.Next() {
		newElem, ok := e.Value.(string)
		if !ok {
			return "", nil, CannotConvertErr(e.Value)
		}
		joinStmt += newElem
	}

	query := fmt.Sprintf("select %v from %v where %v", strings.Join(selectsSlice, ", "), joinStmt, strings.Join(wheresSlice, " and "))

	return query, retArgs, nil
}

func sqlJoin(filter *Filter, list *list.List, usedTables map[string]*list.Element) error {
	firstTable := filter.Tables[0]
	if _, ok := usedTables[firstTable]; !ok {
		return CannotJoinTablesErr(firstTable, keysToString(usedTables))
	}
	for i := 1; i < len(filter.Tables); i++ {
		table := filter.Tables[i]
		if _, ok := usedTables[table]; !ok {
			j := list.InsertAfter(fmt.Sprintf(" join %v", table), usedTables[filter.Tables[i-1]])
			newEl := list.InsertAfter(fmt.Sprintf(" on %v", filter.JoinsOn[i-1]), j)
			usedTables[table] = newEl
		}
	}

	return nil
}

func keysToString(m map[string]*list.Element) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return "[" + strings.Join(keys, ", ") + "]"
}
