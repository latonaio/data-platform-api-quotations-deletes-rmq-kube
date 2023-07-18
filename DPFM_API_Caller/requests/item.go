package requests

type Item struct {
	Quotation          		int     `json:"Quotation"`
	QuotationItem      		int     `json:"QuotationItem"`
	IsMarkedForDeletion		*bool   `json:"IsMarkedForDeletion"`
}
