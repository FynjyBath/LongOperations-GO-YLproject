package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
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

var mu sync.Mutex
var db *sql.DB

func main() {
	db, err := sql.Open("sqlite3", "db")
	if err != nil {
		fmt.Println("Error opening database:", err)
		return
	}
	defer db.Close()

	numGoroutines := getNumGoroutines()

	for i := 0; i < numGoroutines; i++ {
		go startWorker(i)
	}

	select {}
}

func evaluateExpression(expression string) (float64, error) {
	var stack []float64
	tokens := strings.Fields(expression)
	for _, token := range tokens {
		switch token {
		case "+", "-", "*", "/":
			if len(stack) < 2 {
				return 0, fmt.Errorf("недостаточно операндов для операции %s", token)
			}
			operand2 := stack[len(stack)-1]
			operand1 := stack[len(stack)-2]
			stack = stack[:len(stack)-2]
			var result float64
			switch token {
			case "+":
				result = operand1 + operand2
				time.Sleep(time.Second)
			case "-":
				result = operand1 - operand2
				time.Sleep(time.Second)
			case "*":
				result = operand1 * operand2
				time.Sleep(time.Second)
			case "/":
				if operand2 == 0 {
					return 0, fmt.Errorf("деление на ноль")
				}
				result = operand1 / operand2
				time.Sleep(time.Second)
			}
			stack = append(stack, result)

		default:
			num, err := strconv.ParseFloat(token, 64)
			if err != nil {
				return 0, fmt.Errorf("ошибка при парсинге числа %s: %v", token, err)
			}
			stack = append(stack, num)
		}
	}

	if len(stack) != 1 {
		return 0, fmt.Errorf("некорректное выражение")
	}

	return stack[0], nil
}

func getTask() (Task, error) {
	mu.Lock()
	defer mu.Unlock()

	querySQL := "SELECT * FROM tasks WHERE status='submitted' LIMIT 1;"
	row := db.QueryRow(querySQL)
	var task Task
	err := row.Scan(&task.ID, &task.Status, &task.Received, &task.Content, &task.Result, &task.Error)
	if err != nil {
		return Task{}, err
	}

	querySQL = "UPDATE tasks SET status='pending' WHERE id=?;"
	_, err = db.Exec(querySQL, task.ID)
	if err != nil {
		return Task{}, err
	}

	return task, nil
}

func startWorker(workerID int) {
	for {
		task, err := getTask()
		if err != nil {
			fmt.Printf("Worker %d: Error getting task: %v\n", workerID, err)
			time.Sleep(5 * time.Second)
			continue
		}

		var result float64
		go func() {
			result, err = evaluateExpression(task.Content)
		}()

		task.Result = strconv.FormatFloat(result, 'f', -1, 64)
		task.Error = err

		querySQL := "UPDATE tasks SET status='completed', result=?, error=? WHERE id=?;"
		_, err = db.Exec(querySQL, task.Result, task.Error, task.ID)
		if err != nil {
			fmt.Printf("Worker %d: Error sending result: %v\n", workerID, err)
		}
	}
}

func getNumGoroutines() int {
	numGoroutinesStr := os.Getenv("NUM_GOROUTINES")
	if numGoroutinesStr == "" {
		return 1
	}
	numGoroutines, err := strconv.Atoi(numGoroutinesStr)
	if err != nil || numGoroutines <= 0 {
		return 1
	}
	return numGoroutines
}
