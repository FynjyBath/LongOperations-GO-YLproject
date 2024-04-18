package main

import (
	"agent"
	"database/sql"
	"fmt"
	"html/template"
	"net/http"
	"orchestrator"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var MU sync.Mutex
var DB *sql.DB

const hmacSampleSecret = "ILoveUlyanovskVeryMuch"

func AddWorker(w http.ResponseWriter, r *http.Request) {
	go agent.StartWorker()
	tokenString := r.URL.Query().Get("jwt_token")
	http.Redirect(w, r, "/checkWorkers?jwt_token=" + tokenString, http.StatusSeeOther)
}

func Register(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFiles("../templates/register.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := struct{ Message string }{Message: ""}
	err = tmpl.Execute(w, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func ReceiveRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Error parsing form", http.StatusInternalServerError)
		return
	}

	login := r.FormValue("login")
	password := r.FormValue("password")

	flag := true

	tx, err := DB.Begin()
	if err != nil {
		fmt.Fprint(w, err.Error())
		return
	}

	insertDataSQL := "INSERT INTO users VALUES (?, ?);"
	_, err = DB.Exec(insertDataSQL, login, password)
	if err != nil {
		flag = false
	}

	mp := make(map[rune]int)
	mp['+'] = 1
	mp['-'] = 1
	mp['*'] = 1
	mp['/'] = 1
	for op, num := range mp {
		insertDataSQL = "INSERT INTO times VALUES (?, ?, ?);"
		DB.Exec(insertDataSQL, op, num, login)
	}

	if err := tx.Commit(); err != nil {
		flag = false
	}

	if flag {
		http.Redirect(w, r, "/", http.StatusSeeOther)
	} else {
		tmpl, err := template.ParseFiles("../templates/register.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data := struct{ Message string }{Message: "Такой логин уже использован."}
		err = tmpl.Execute(w, data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func Login(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFiles("../templates/login.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := struct{ Message string }{Message: ""}
	err = tmpl.Execute(w, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func ReceiveLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Error parsing form", http.StatusInternalServerError)
		return
	}

	login := r.FormValue("login")
	correct_password := r.FormValue("password")

	flag := true

	MU.Lock()

	querySQL := "SELECT password FROM users WHERE login = ?;"
	rows, err := DB.Query(querySQL, login)
	if err != nil {
		flag = false
	}
	var password string
	rows.Next()
	err = rows.Scan(&password)
	if err != nil {
		flag = false
	}
	rows.Close()

	MU.Unlock()

	if password == correct_password && flag {
		now := time.Now()
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
			"login": login,
			"nbf":   now.Unix(),
			"exp":   now.Add(30 * time.Minute).Unix(),
			"iat":   now.Unix(),
		})

		tokenString, err := token.SignedString([]byte(hmacSampleSecret))
		if err != nil {
			panic(err)
		}

		http.Redirect(w, r, "/getTasks?jwt_token="+tokenString, http.StatusSeeOther)
	} else {
		tmpl, err := template.ParseFiles("../templates/login.html")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		data := struct{ Message string }{Message: "Неверные имя пользователя или пароль."}
		err = tmpl.Execute(w, data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
}

func main() {
	var err error

	orchestrator.MU = &MU
	agent.MU = &MU

	DB, err = sql.Open("sqlite3", "../db.db")

	orchestrator.DB = DB
	if err != nil {
		fmt.Println("Error opening database:", err)
		return
	}

	/*_, err = orchestrator.DB.Exec("DELETE FROM tasks;")
	if err != nil {
		return
	}*/
	// раскомментить чтобы удалять старое перед запуском

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

	http.HandleFunc("/", Login)
	http.HandleFunc("/receiveLogin", ReceiveLogin)
	http.HandleFunc("/register", Register)
	http.HandleFunc("/receiveRegister", ReceiveRegister)
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
