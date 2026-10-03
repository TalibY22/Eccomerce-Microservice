package domain

type OrderItem struct {
	ProductID   string
	SKU         string
	ProductName string
	Quantity    int32
	UnitPrice   float64
}

type Order struct {
	ID          string
	UserID      string
	Currency    string
	Items       []OrderItem
	TotalAmount float64
	Status      string
}
