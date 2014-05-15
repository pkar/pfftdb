package pfftdb

import (
	"net/http"
)

var maps chan map[string]interface{}

func init() {
	maps = make(chan map[string]interface{})
	go mapStringInterfaceGenerator(maps)
}

func mapStringInterfaceGenerator(m chan map[string]interface{}) {
	for {
		m <- map[string]interface{}{"code": 200, "err": ""}
	}
}

func methodNotAllowed(m string) map[string]interface{} {
	err := <-maps
	err["code"] = http.StatusMethodNotAllowed
	err["err"] = "Method not allowed: " + m
	return err
}

func internalServerError(e string) map[string]interface{} {
	err := <-maps
	err["code"] = http.StatusInternalServerError
	err["err"] = e
	return err
}

func badRequest(e string) map[string]interface{} {
	err := <-maps
	err["code"] = http.StatusBadRequest
	err["err"] = e
	return err
}
