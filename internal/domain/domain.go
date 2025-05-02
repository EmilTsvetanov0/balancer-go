package domain

type Client struct {
	Id       string `json:"id"`
	Capacity int    `json:"capacity"`
	Rate     int    `json:"rate"`
}

// --- Errors ---

type ErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
