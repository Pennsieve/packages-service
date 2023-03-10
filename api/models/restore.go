package models

type RestoreRequest struct {
	NodeIds []string `json:"nodeIds"`
}

type RestoreResponse struct {
	Success  []string  `json:"success"`
	Failures []Failure `json:"failures"`
}

type Failure struct {
	Id    string `json:"id"`
	Error string `json:"error"`
}

type RestorePackageMessage RestoreRequest
