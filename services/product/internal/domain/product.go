package domain

type Product struct {
	ID          string
	SKU         string
	Name        string
	Description string
	Price       float64
	Category    string
	Currency    string
	Active      bool
}
