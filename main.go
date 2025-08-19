package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/segmentio/kafka-go"

	_ "github.com/lib/pq"
)

type Order struct {
	OrderUID        string   `json:"order_uid"`
	TrackNumber     string   `json:"track_number"`
	Entry           string   `json:"entry"`
	Delivery        Delivery `json:"delivery"`
	Payment         Payment  `json:"payment"`
	Items           []Item   `json:"items"`
	Locale          string   `json:"locale"`
	CustomerID      string   `json:"customer_id"`
	DeliveryService string   `json:"delivery_service"`
	DateCreated     string   `json:"date_created"`
}

type Delivery struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

type Payment struct {
	Transaction  string `json:"transaction"`
	Currency     string `json:"currency"`
	Provider     string `json:"provider"`
	Amount       int    `json:"amount"`
	PaymentDt    int    `json:"payment_dt"`
	Bank         string `json:"bank"`
	DeliveryCost int    `json:"delivery_cost"`
	GoodsTotal   int    `json:"goods_total"`
}

type Item struct {
	ChrtID      int    `json:"chrt_id"`
	TrackNumber string `json:"track_number"`
	Price       int    `json:"price"`
	Name        string `json:"name"`
	Size        string `json:"size"`
	TotalPrice  int    `json:"total_price"`
	NmID        int    `json:"nm_dt"`
	Brand       string `json:"brand"`
}

var cache = make(map[string]Order) //кэш для хранения заказов

// saveOrder структуры и функции

func getOrder(db *sql.DB, orderUID string) (*Order, error) { // получение заказа

	if cacheOrder, ok := cache[orderUID]; ok {
		fmt.Printf("Заказ %s найден в кэше\n", orderUID)
		return &cacheOrder, nil
	}

	var order Order
	var jsonData []byte
	err := db.QueryRow("SELECT data FROM orders WHERE order_uid = $1", orderUID).Scan(&jsonData)
	if err != nil {
		return nil, fmt.Errorf("ошибка плучения заказа: %v", err)
	}
	err = json.Unmarshal(jsonData, &order)
	if err != nil {
		return nil, fmt.Errorf("ошибка парсинга Json: %v", err)
	}
	return &order, nil
}

var db *sql.DB

func handleGetOrder(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/order/")
	order, err := getOrder(db, id)
	if err != nil {
		http.Error(w, "Заказ не найден", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(order)
}

func saveOrder(db *sql.DB, order Order) error {
	jsonData, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("ошибка сериализации JSON: %v", err)
	}
	_, err = db.Exec("INSERT INTO orders (order_uid, data) VALUES ($1, $2) ON CONFLICT (order_uid) DO UPDATE SET data = EXCLUDED.data", order.OrderUID, jsonData)
	if err != nil {
		return fmt.Errorf("ошибка сохранения заказа: %v", err)
	}

	cache[order.OrderUID] = order
	fmt.Printf("Заказ %s сохранён в БД и кэше\n", order.OrderUID)

	return nil
}

func consumeFromKafka() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "orders",
		GroupID: "my-group",
	})
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Ошибка чтения из Kafka: %v", err)
			continue
		}

		var order Order
		err = json.Unmarshal(m.Value, &order)
		if err != nil {
			log.Printf("Ошибка парсинга JSONL: %v", err)
			continue
		}
		err = saveOrder(db, order)
		if err != nil {
			log.Printf("Ошибка сохранения заказа: %v", err)
			continue
		}
		log.Printf("Заказ %s успешно сохранен из Kafka", order.OrderUID)
	}
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "index.html")
}

func main() {
	//PostgreSQL
	var err error
	db, err = sql.Open("postgres", "user=user password=pass dbname=orders_db sslmode=disable host=localhost port=5433")
	if err != nil {
		log.Fatal("Ошибка подключения к БД:", err)
	}
	defer db.Close()

	log.SetFlags(log.LstdFlags | log.Lshortfile) // ошибки

	rows, err := db.Query("SELECT order_uid, data FROM orders") // загрузка кэша
	if err != nil {
		log.Fatal("Ошибка загрузки кэша  из БД:", err)
	}
	defer rows.Close()

	cacheCount := 0

	for rows.Next() {
		var orderUID string
		var jsonData []byte
		err := rows.Scan(&orderUID, &jsonData)
		if err != nil {
			log.Printf("Ошибка чтения данных из БД: %v", err)
			continue
		}
		var order Order // переменная ордер
		err = json.Unmarshal(jsonData, &order)
		if err != nil {
			log.Printf("Ошибка парсинга JSON: %v", err)
			continue
		}

		cache[orderUID] = order
		cacheCount++
	}

	if err := rows.Err(); err != nil {
		log.Fatal("Ошибка при чтении строк из БД:", err)
	}

	fmt.Printf("Кэш загружен: %d заказов\n", cacheCount)

	err = db.Ping() // Проверяем соединение
	if err != nil {
		log.Fatal("Не могу подключиться к БД:", err)
	}
	fmt.Println("Успешно подключились к PostgreSQL!")

	// Создаём таблицу

	_, err = db.Exec(`                                
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            order_uid TEXT UNIQUE NOT NULL,
            data JSONB
        );
    `)
	if err != nil {
		log.Fatal("Ошибка создания таблицы:", err)
	}
	fmt.Println("✅ Таблица orders создана или уже существует")

	go consumeFromKafka() //запуск Кафка

	//Создаем заказ
	testOrder := Order{
		OrderUID:    "test-order-123",
		TrackNumber: "WBILMTESTTRACK",
		Entry:       "WBIL",
		Delivery: Delivery{
			Name:    "Test User",
			Phone:   "+79991234567",
			Zip:     "123456",
			City:    "Moscow",
			Address: "Street 1",
			Region:  "Moscow Obl",
			Email:   "test@mail.com",
		},
		Payment: Payment{
			Transaction:  "b5a2b7a2-1e9d-4c9d-8f1c-1a2b3c4d5e6f",
			Currency:     "USD",
			Provider:     "wbpay",
			Amount:       317,
			PaymentDt:    1637907727,
			Bank:         "alpha",
			DeliveryCost: 1500,
			GoodsTotal:   317,
		},
		Items: []Item{
			{
				ChrtID:      999999,
				TrackNumber: "WBILMTESTTRACK",
				Price:       317,
				Name:        "Test Item",
				Size:        "L",
				TotalPrice:  317,
				NmID:        111111,
				Brand:       "Test Brand",
			},
		},
		Locale:          "en",
		CustomerID:      "test",
		DeliveryService: "meest",
		DateCreated:     "2021-11-26T06:22:19Z",
	}

	// Сохранение заказа
	err = saveOrder(db, testOrder)
	if err != nil {
		log.Fatal("Ошибка сохранения заказа:", err)
	}

	fmt.Println("Заказ успешно сохранён в PostgreSQL!")

	// запускHTTP-сервера
	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/order/", handleGetOrder)
	fmt.Println("HTTP-сервер запущен на http:/localhost:8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
