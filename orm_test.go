package arangorm

import (
	"fmt"
	"strings"
	"testing"
)

var app *Instance

func init() {
	instance, err := New(Config{
		Hosts: []string{"http://localhost:8529"},
		User:  "root",
		Pass:  "alojomora",
		Db:    "videochat_test",
	}, "videoChatTestGraph")

	if err != nil {
		panic(err)
	}

	app = instance
}

func TestTraversalQuery(t *testing.T) {
	queryExample1 := clean(`
	 FOR airport, flight IN INBOUND 'airports/BIS' flights
		FILTER flight.Month == @Month1
			OR flight.Day >= @Day2
			AND flight.Day <= @Day3
		LIMIT @_limit
	 RETURN { 
		city: airport.city, 
		time: flight.ArrTimeUTC,
		airport
	 }`)
	paramsExample1 := map[string]interface{}{
		"Month1": 1,
		"Day2":   5,
		"Day3":   7,
		"_limit": 100,
	}

	q := app.Query("flights").Inbound(GetId("airports", "BIS")).Limit(100)

	q.NameVertex("airport")
	q.NameEdge("flight")

	q.Filter(FilterOption{
		Target:   q.Edge,
		Property: "Month",
		Value:    1,
	}).Or(FilterOption{
		Target:    q.Edge,
		Property:  "Day",
		Operation: ">=",
		Value:     5,
	}).And(FilterOption{
		Target:    q.Edge,
		Property:  "Day",
		Operation: "<=",
		Value:     7,
	})

	q.Return(fmt.Sprintf("city: %s.city", q.Vertex))
	q.Return(fmt.Sprintf("time: %s.ArrTimeUTC", q.Edge))
	q.Return(q.Vertex.String())

	query, params, err := q.getMetadata()

	if err != nil {
		t.Error(err)
	}

	t.Log(query)

	if !compareMaps(paramsExample1, params) {
		t.Error("EXAMPLE 1 MAPS DISTINCT")
	}
	if clean(query) != queryExample1 {
		t.Error("EXAMPLE 1 FAIL")
	}
}

// UTILS

func clean(s string) string {
	result := s
	result = strings.ReplaceAll(result, "\n", "")
	result = strings.ReplaceAll(result, "\t", "")
	result = strings.ReplaceAll(result, " ", "")
	return result
}

func compareMaps(map0, map1 map[string]interface{}) bool {
	for key, val := range map0 {
		if val2, ok := map1[key]; !ok || val2 != val {
			return false
		}
	}
	for key, val := range map1 {
		if val2, ok := map0[key]; !ok || val2 != val {
			return false
		}
	}
	return true
}
