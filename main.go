package main

import (
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"topology/application/handlers"
	"topology/application/services"
)

func main() {
	nodeService := services.NewNodeService()
	err := nodeService.MonitorTopologyHealth()

	if err != nil {
		log.Fatal(err)
	}

	r := mux.NewRouter()
	r.HandleFunc("/api/v1/join", handlers.JoinHandler).Methods("POST")

	http.Handle("/", r)

	fmt.Println("Server listen on port 8080...")
	err = http.ListenAndServe("0.0.0.0:8080", r)

	if err != nil {
		log.Fatal(err)
	}
}
