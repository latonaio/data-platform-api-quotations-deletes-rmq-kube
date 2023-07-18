package requests

type Header struct {
	Quotation            int     `json:"Quotation"`
	IsMarkedForDeletion  *bool   `json:"IsMarkedForDeletion"`
}
