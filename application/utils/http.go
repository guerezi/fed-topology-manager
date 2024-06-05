package utils

type HTTPResponse struct {
	Status      string      `json:"status"`
	Code        int         `json:"code"`
	Data        interface{} `json:"data"`
	Description string      `json:"description"`
}
