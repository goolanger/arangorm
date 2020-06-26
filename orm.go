package arangorm

import (
	"errors"
	"fmt"
	"strings"

	"github.com/arangodb/go-driver"
)

type QueryElement struct {
	name string
}

func (e QueryElement) String() string {
	return e.name
}
func CreateQueryElement(name string) QueryElement {
	return QueryElement{name}
}

type queryType string

var queryTypes = struct {
	document queryType
	edge     queryType
}{
	"doc",
	"edge",
}

type traversalType string

var traversalTypes = struct {
	Inbound  traversalType
	Outbound traversalType
	Any      traversalType
}{
	"INBOUND",
	"OUTBOUND",
	"ANY",
}

type filterType string

var filterTypes = struct {
	filter filterType
	and    filterType
	or     filterType
}{
	"FILTER",
	"\tAND",
	"\tOR",
}

type FilterOption struct {
	Target    QueryElement
	Property  string
	Operation string
	Value     interface{}
}

type Filter struct {
	FilterOption
	filters []*Filter
	_type   filterType
}

type Query struct {
	app             *Instance
	collection      string
	query           queryType
	traversal       traversalType
	traversalDeep   string
	traversalVertex driver.DocumentID
	limit           int
	returns         []string
	filters         []*Filter
	Edge            QueryElement
	Vertex          QueryElement
	Document        QueryElement
}

func (app *Instance) Query(collection string) *Query {
	return &Query{
		app:        app,
		collection: collection,
		Vertex:     CreateQueryElement("vertex"),
		Edge:       CreateQueryElement("edge"),
		Document:   CreateQueryElement("document"),
	}
}

func (q *Query) NameDocument(name string) *Query {
	q.Document = CreateQueryElement(name)
	return q
}

func (q *Query) NameVertex(name string) *Query {
	q.Vertex = CreateQueryElement(name)
	return q
}

func (q *Query) NameEdge(name string) *Query {
	q.Edge = CreateQueryElement(name)
	return q
}

func (q *Query) Inbound(v driver.DocumentID) *Query {
	return q.queryVertex(traversalTypes.Inbound, v)
}

func (q *Query) Outbound(v driver.DocumentID) *Query {
	return q.queryVertex(traversalTypes.Outbound, v)
}

func (q *Query) Any(v driver.DocumentID) *Query {
	return q.queryVertex(traversalTypes.Any, v)
}

func (q *Query) queryVertex(t traversalType, v driver.DocumentID) *Query {
	q.query = queryTypes.edge
	q.traversalVertex = v
	q.traversal = t
	return q
}

func (q *Query) Limit(l int) *Query {
	q.limit = l
	return q
}

func (q *Query) Return(r string) *Query {
	if q.returns == nil {
		q.returns = []string{r}
	} else {
		q.returns = append(q.returns, r)
	}
	return q
}

func (q *Query) Filter(option FilterOption) *Filter {
	filter := Filter{
		_type:        filterTypes.filter,
		FilterOption: option,
	}

	if q.filters == nil {
		q.filters = []*Filter{&filter}
	} else {
		q.filters = append(q.filters, &filter)
	}

	return &filter
}

func (f *Filter) Or(option FilterOption) *Filter {
	return f.push(filterTypes.or, option)
}

func (f *Filter) And(option FilterOption) *Filter {
	return f.push(filterTypes.and, option)
}

func (f *Filter) push(filter filterType, option FilterOption) *Filter {
	result := Filter{
		_type:        filter,
		FilterOption: option,
	}

	if f.filters == nil {
		f.filters = []*Filter{&result}
	} else {
		f.filters = append(f.filters, &result)
	}

	return f
}

func (f *Filter) getMetadata(q *Query, index *int) (string, map[string]interface{}, error) {
	var (
		query  = ""
		params = make(map[string]interface{})
	)

	query += fmt.Sprintf("%s\t", f._type)

	*index++
	key := fmt.Sprintf("%s%d", f.Property, *index)
	var operator string
	if operator = f.Operation; operator == "" {
		operator = "=="
	}
	var elem QueryElement
	if elem = f.Target; elem.name == "" {
		elem = q.Document
	}
	query += fmt.Sprintf("%s.%s\t%s\t@%s\n", elem.String(), f.Property, operator, key)
	params[key] = f.Value

	if f.filters != nil {
		for _, filter := range f.filters {
			q, p, err := filter.getMetadata(q, index)
			if err != nil {
				return "", nil, err
			}
			if params, err = mergeParams(&params, &p); err != nil {
				return "", nil, err
			}
			query += q
		}
	}

	return query, params, nil
}

func mergeParams(p1, p2 *map[string]interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for k, v := range *p1 {
		result[k] = v
	}
	for k, v := range *p2 {
		if _, ok := result[k]; ok {
			return nil, fmt.Errorf("duplication of key: %s", k)
		}
		result[k] = v
	}
	return result, nil
}

// TODO: Create new filter
func (q *Query) Execute(result interface{}) error {
	query, params, err := q.getMetadata()
	if err != nil {
		return err
	}

	return q.app.Execute(query, params, result)
}

// GET METADATA FUNCTION
func (q *Query) getMetadata() (string, map[string]interface{}, error) {

	var (
		query  = ""
		params = make(map[string]interface{})
	)

	query += "\nFOR\t"

	switch q.query {
	case queryTypes.edge:
		query += fmt.Sprintf("%s, %s\t", q.Vertex, q.Edge)
	case queryTypes.document:
	default:
		query += fmt.Sprintf("%s\t", q.Document)
	}

	query += "IN\t"

	switch q.query {
	case queryTypes.edge:
		if q.traversalVertex.IsEmpty() {
			return "", nil, errors.New("missing directive: can not query empty vertex")
		} else {
			var traversal traversalType
			if traversal = q.traversal; traversal == "" {
				traversal = traversalTypes.Inbound
			}
			query += fmt.Sprintf("%s\t%s\t'%s'\t", traversal, q.traversalDeep, q.traversalVertex.String())
		}
	case queryTypes.document:
	default:
	}

	query += fmt.Sprintf("%s\n", q.collection)

	var varCount = 0
	if q.filters != nil {
		for _, f := range q.filters {
			filter, param, err := f.getMetadata(q, &varCount)
			if err != nil {
				return "", nil, err
			}
			if params, err = mergeParams(&params, &param); err != nil {
				return "", nil, err
			}
			query += filter
		}
	}

	if q.limit > 0 {
		query += "LIMIT @_limit\n"
		params["_limit"] = q.limit
	}

	query += "RETURN\t"
	if q.returns == nil {
		switch q.query {
		case queryTypes.edge:
			query += fmt.Sprintf("{%s, %s}", q.Vertex, q.Edge)
		case queryTypes.document:
		default:
			query += q.Document.String()
		}
	} else {
		query += "{\n\t"
		query += strings.Join(q.returns, ",\n\t")
		query += "\n}\n"
	}

	return query, params, nil
}
