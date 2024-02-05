package main

import (
	"database/sql"
	"fmt"
	"net/http"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

type Task struct {
	ID       string    `json:"id"`
	Status   string    `json:"status"`
	Received time.Time `json:"received"`
	Content  string    `json:"content"`
	Result   string    `json:"result,omitempty"`
	Error    error     `json:"error"`
}

var tasks = make(map[string]Task)
var mu sync.Mutex

func addExpression(w http.ResponseWriter, r *http.Request) {
	content := r.URL.Query().Get("expression")

	if content == "" {
		http.Error(w, "Missing 'expression' parameter", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	taskID := generateID()
	tasks[taskID] = Task{
		ID:       taskID,
		Content:  content,
		Status:   "submitted",
		Received: time.Now(),
	}

	insertDataSQL := "INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?);"
	_, err := db.Exec(insertDataSQL, taskID, "submitted", time.Now(), content, 0, nil)
	if err != nil {
		fmt.Println("Error inserting data:", err)
		return
	}

	fmt.Fprintf(w, "Expression '%s' submitted. Check status later with ID: %s\n", content, taskID)
}

func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func main() {
	db, err := sql.Open("sqlite3", "db")
	if err != nil {
		fmt.Println("Error opening database:", err)
		return
	}
	defer db.Close()

	http.HandleFunc("/addExpression", addExpression)

	fmt.Println("Orchestrator listening on :8081...")
	http.ListenAndServe(":8081", nil)
}
