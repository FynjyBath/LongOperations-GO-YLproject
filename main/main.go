package main

import (
	"agent"
	"database/sql"
	"fmt"
	"net/http"
	"orchestrator"
	"sync"
)

var MU sync.Mutex

func AddWorker(w http.ResponseWriter, r *http.Request) {
	go agent.StartWorker()
	http.Redirect(w, r, "/checkWorkers", http.StatusSeeOther)
}

func main() {
	var err error

	orchestrator.MU = &MU
	agent.MU = &MU

	orchestrator.DB, err = sql.Open("sqlite3", "../db.db")
	if err != nil {
		fmt.Println("Error opening database:", err)
		return
	}

	_, err = orchestrator.DB.Exec("DELETE FROM tasks;")
	if err != nil {
		return
	}

	_, err = orchestrator.DB.Exec("DELETE FROM workers;")
	if err != nil {
		return
	}
	fmt.Println("Все старые workers удалены успешно.")

	_, err = orchestrator.DB.Exec("UPDATE tasks SET status = 'submitted' WHERE status = 'pending';")
	if err != nil {
		return
	}
	fmt.Println("Все упавшие при вычислении записи восстановлены (если такие имелись).")

	agent.DB = orchestrator.DB
	defer orchestrator.DB.Close()

	go orchestrator.ValidTasks()
	fmt.Println("Goroutine with checking task started")
	go agent.StartWorker()
	fmt.Println("Goroutines with workers started: 1")

	http.HandleFunc("/addExpression", orchestrator.AddExpression)
	http.HandleFunc("/receiveExpression", orchestrator.ReceiveExpression)
	http.HandleFunc("/getTasks", orchestrator.GetTasks)
	http.HandleFunc("/changeTimes", orchestrator.ChangeTimes)
	http.HandleFunc("/receiveTimes", orchestrator.ReceiveTimes)
	http.HandleFunc("/checkWorkers", orchestrator.CheckWorkers)
	http.HandleFunc("/addWorker", AddWorker)

	fmt.Println("Orchestrator listening on :8081...")
	http.ListenAndServe(":8081", nil)
}
