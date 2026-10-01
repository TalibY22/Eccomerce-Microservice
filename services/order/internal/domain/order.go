package domain



type OrderItem struct{
	ProductID string
	Quantity int32
	UnitPrice float64
}

type Order struct{
     ID string
	 UserID string
	 Items []OrderItem
	 TotalAmount float64
	 Status string
}