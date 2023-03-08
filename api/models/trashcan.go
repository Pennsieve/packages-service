package models

type TrashcanPage struct {
	Limit      int            `json:"limit"`
	Offset     int            `json:"offset"`
	TotalCount int            `json:"totalCount"`
	Packages   []TrashcanItem `json:"packages"`
	Messages   []string       `json:"messages"`
}

type TrashcanItem struct {
	ID     int64  `json:"id"`
	Name   string `json:"name"`
	NodeId string `json:"node_id"`
	Type   string `json:"type"`
	State  string `json:"state"`
}
