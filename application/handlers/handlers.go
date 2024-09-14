package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"topology/application/services"
	"topology/application/utils"
)

// JoinHandler function
// This function is a handler that receives a POST request to join a node to the network
// It returns a JSON response with the federator configuration
func JoinHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Request on JoinHandler: ", r)

	// Create a new NodeService
	nodeService := services.NewNodeService()

	// Create a new Node with the request body
	federatorConfig, err := nodeService.NewNode(r.Body)

	var response utils.HTTPResponse

	// If there is an error, return a JSON response with the error
	// else, return a JSON response with the federator configuration
	if err != nil {
		response.Code = 400
		response.Status = "error"
		response.Description = err.Error()
		w.WriteHeader(http.StatusBadRequest)
	} else {
		response.Code = 200
		response.Status = "success"
		response.Data = federatorConfig
		w.WriteHeader(http.StatusOK)
	}

	// Write the response to the response writer
	payload, _ := json.Marshal(response)

	// Write the response to the response writer
	_, err = w.Write(payload)

	fmt.Println("Response on JoinHandler: ", string(payload))

	if err != nil {
		fmt.Println("Error on write response")
	}
}
