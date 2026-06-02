package api

import (
	"encoding/json"
	"github.com/m1sol/go-event-pipeline-lab/internal/orders"
	"net/http"
)

type OrdersHandler struct {
	service *orders.Service
}

func NewOrdersHandler(service *orders.Service) *OrdersHandler {
	return &OrdersHandler{service: service}
}

type createOrderRequest struct {
	MessageID string `json:"message_id"`
	UserID    string `json:"user_id"`
	Amount    int64  `json:"amount"`
}

func (h *OrdersHandler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.UserID == "" || req.Amount <= 0 {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	event, err := h.service.CreateOrder(r.Context(), orders.CreateOrderInput{
		MessageID: req.MessageID,
		UserID:    req.UserID,
		Amount:    req.Amount,
	})
	if err != nil {
		http.Error(w, "failed to create order event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(event)
}
