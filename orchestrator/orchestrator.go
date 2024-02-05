package orchestrator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Task struct {
	ID       string    `json:"id"`
	Status   string    `json:"status"`
	Received time.Time `json:"received"`
	Content  string    `json:"content"`
	Result   string    `json:"result,omitempty"`
	Error    error     `json:"error"`
}

func addExpression(w http.ResponseWriter, r *http.Request) {
	content := r.URL.Query().Get("expression")

	if content == "" {
		http.Error(w, "Missing 'expression' parameter", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	taskID := generateID()

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

func getTasks(w http.ResponseWriter, r *http.Request) {
	querySQL := "SELECT * FROM tasks;"
	mu.Lock()
	rows, err := db.Query(querySQL)
	if err != nil {
		fmt.Println("Error querying data:", err)
		return
	}
	defer rows.Close()

	var ret []Task
	for rows.Next() {
		var task Task
		err := rows.Scan(&task.ID, &task.Status, &task.Received, &task.Content, &task.Result, &task.Error)
		if err != nil {
			fmt.Fprintf(w, "Error scanning")
			return
		}
		ret = append(ret, task)
	}
	mu.Unlock()

	response, _ := json.Marshal(ret)
	fmt.Fprint(w, response)
}

func main() {
	http.HandleFunc("/addExpression", addExpression)
	http.HandleFunc("/getTasks", getTasks)

	fmt.Println("Orchestrator listening on :8081...")
	http.ListenAndServe(":8081", nil)
}
