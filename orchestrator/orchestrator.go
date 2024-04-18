package orchestrator

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"text/template"
	"time"

	"github.com/golang-jwt/jwt/v5"
	_ "github.com/mattn/go-sqlite3"
)

type Task struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Received string `json:"received"`
	Content  string `json:"content"`
	Result   string `json:"result,omitempty"`
	Error    string `json:"error"`
	User     string `json:"user"`
}

type Operation struct {
	name rune
	t    string
}

type Worker struct {
	id      int
	task_id int
}

var DB *sql.DB
var MU *sync.Mutex

const hmacSampleSecret = "ILoveUlyanovskVeryMuch"

func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func ReceiveTimes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Error parsing form", http.StatusInternalServerError)
		return
	}

	tokenString := r.FormValue("jwt_token")
	tokenFromString, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(hmacSampleSecret), nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	claims, ok := tokenFromString.Claims.(jwt.MapClaims)
	if !ok {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	login := claims["login"]

	tx, err := DB.Begin()
	if err != nil {
		fmt.Fprintf(w, err.Error())
		return
	}

	mp := make(map[rune]int)
	mp['+'], _ = strconv.Atoi(r.FormValue("number1"))
	mp['-'], _ = strconv.Atoi(r.FormValue("number2"))
	mp['*'], _ = strconv.Atoi(r.FormValue("number3"))
	mp['/'], _ = strconv.Atoi(r.FormValue("number4"))

	for op, num := range mp {
		insertDataSQL := "INSERT OR IGNORE INTO times VALUES (?, ?, ?);"
		_, err = DB.Exec(insertDataSQL, op, num, login)
		insertDataSQL = "UPDATE times SET time=? WHERE operation=? AND user=?;"
		_, err = DB.Exec(insertDataSQL, num, op, login)
	}

	if err := tx.Commit(); err != nil {
		fmt.Fprintf(w, err.Error())
		return
	}

	http.Redirect(w, r, "/getTasks?jwt_token="+tokenString, http.StatusSeeOther)
}

func ChangeTimes(w http.ResponseWriter, r *http.Request) {
	tokenString := r.URL.Query().Get("jwt_token")
	tokenFromString, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(hmacSampleSecret), nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	claims, ok := tokenFromString.Claims.(jwt.MapClaims)
	if !ok {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	login := claims["login"]

	(*MU).Lock()
	var op Operation
	mp := make(map[rune]int)
	querySQL := "SELECT operation, time FROM times WHERE user = ?;"
	rows, err := DB.Query(querySQL, login)
	if err != nil {
		(*MU).Unlock()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	mp['+'] = 1
	mp['-'] = 1
	mp['/'] = 1
	mp['*'] = 1
	for rows.Next() {
		rows.Scan(&op.name, &op.t)
		res, err := strconv.Atoi(op.t)
		if err == nil {
			mp[op.name] = res
		}
	}
	rows.Close()
	(*MU).Unlock()

	s := fmt.Sprintf(`<form id="numberForm" action="/receiveTimes" method="post">
        <div>
            <label for="number1">+:</label>
            <input type="number" id="number1" name="number1" value="%d" required>
        </div>
        <div>
            <label for="number2">-:</label>
            <input type="number" id="number2" name="number2" value="%d" required>
        </div>
        <div>
            <label for="number3">*:</label>
            <input type="number" id="number3" name="number3" value="%d" required>
        </div>
        <div>
            <label for="number4">/:</label>
            <input type="number" id="number4" name="number4" value="%d" required>
        </div>
		<div>
            <label for="jwt_token">Token:</label>
            <input type="string" id="jwt_token" name="jwt_token" value="%s" readonly required>
        </div>
        <button type="submit">Изменить</button>
    </form>`, mp['+'], mp['-'], mp['*'], mp['/'], tokenString)

	data := struct {
		Title   string
		Header  string
		Content string
		Token   string
	}{
		Title:   "Время выполнения",
		Header:  "Изменить время выполнения (в секундах):",
		Content: s,
		Token:   tokenString,
	}

	tmpl, err := template.ParseFiles("../templates/template.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = tmpl.Execute(w, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func ReceiveExpression(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	content := r.FormValue("inputValue")

	if content == "" {
		http.Error(w, "Missing 'expression' parameter", http.StatusBadRequest)
		return
	}

	tokenString := r.FormValue("jwt_token")
	tokenFromString, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(hmacSampleSecret), nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	claims, ok := tokenFromString.Claims.(jwt.MapClaims)
	if !ok {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	login := claims["login"]

	(*MU).Lock()
	defer (*MU).Unlock()

	taskID := generateID()

	tx, err := DB.Begin()
	if err != nil {
		fmt.Fprintf(w, err.Error())
		return
	}

	insertDataSQL := "INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, ?);"
	_, err = DB.Exec(insertDataSQL, taskID, "submitted", time.Now(), content, 0, "", login)
	if err != nil {
		fmt.Fprintf(w, err.Error())
		return
	}

	if err := tx.Commit(); err != nil {
		fmt.Fprintf(w, err.Error())
		return
	}

	http.Redirect(w, r, "/getTasks?jwt_token="+tokenString, http.StatusSeeOther)
}

func AddExpression(w http.ResponseWriter, r *http.Request) {
	tokenString := r.URL.Query().Get("jwt_token")
	tokenFromString, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(hmacSampleSecret), nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	_, ok := tokenFromString.Claims.(jwt.MapClaims)
	if !ok {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	s := fmt.Sprintf(`<form id="myForm" action="/receiveExpression" method="post">
				<div>
				    <label for="inputValue">Введите арифметическое выражение:</label>
					<input type="text" id="inputValue" name="inputValue">
				</div>
				<div>
					<label for="jwt_token">Token:</label>
					<input type="string" id="jwt_token" name="jwt_token" value="%s" readonly required>
				</div>
				<button class="submit">Добавить задачу</button>
			</form>`, tokenString)

	data := struct {
		Title   string
		Header  string
		Content string
		Token   string
	}{
		Title:   "Добавить задачу",
		Header:  "",
		Content: s,
		Token:   tokenString,
	}

	tmpl, err := template.ParseFiles("../templates/template.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = tmpl.Execute(w, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func GetTasks(w http.ResponseWriter, r *http.Request) {
	tokenString := r.URL.Query().Get("jwt_token")
	tokenFromString, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(hmacSampleSecret), nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	claims, ok := tokenFromString.Claims.(jwt.MapClaims)
	if !ok {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	login := claims["login"]

	(*MU).Lock()
	defer (*MU).Unlock()

	querySQL := "SELECT * FROM tasks WHERE user=?;"
	rows, err := DB.Query(querySQL, login)
	if err != nil {
		fmt.Println("Error querying data:", err)
		return
	}
	defer rows.Close()

	var ret []Task
	for rows.Next() {
		var task Task
		err := rows.Scan(&task.ID, &task.Status, &task.Received, &task.Content, &task.Result, &task.Error, &task.User)
		if err != nil {
			fmt.Fprintf(w, "Error scanning: "+err.Error())
			return
		}
		ret = append(ret, task)
	}

	s := `<table>
			<thead>
				<tr>
					<th>ID</th>
					<th>Status</th>
					<th>Received</th>
					<th>Content</th>
					<th>Result</th>
					<th>Error</th>
				</tr>
			</thead>
			<tbody id="taskTableBody">`
	for _, task := range ret {
		s += "<tr>"
		s += "<td>" + string(task.ID) + "</td>"
		s += "<td>" + string(task.Status) + "</td>"
		s += "<td>" + string(task.Received) + "</td>"
		if len(task.Content) < 100 {
			s += "<td>" + string(task.Content) + "</td>"
		} else {
			s += "<td>" + string(task.Content[:97]) + "...</td>"
		}
		s += "<td>" + string(task.Result) + "</td>"
		s += "<td>" + string(task.Error) + "</td>"
		s += "</tr>"
	}
	s += `	</tbody>
		</table>`

	data := struct {
		Title   string
		Header  string
		Content string
		Token   string
	}{
		Title:   "Задачи",
		Header:  "Архив задач:",
		Content: s,
		Token:   tokenString,
	}

	tmpl, err := template.ParseFiles("../templates/template.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = tmpl.Execute(w, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func CheckWorkers(w http.ResponseWriter, r *http.Request) {
	tokenString := r.URL.Query().Get("jwt_token")
	tokenFromString, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(hmacSampleSecret), nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	_, ok := tokenFromString.Claims.(jwt.MapClaims)
	if !ok {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	(*MU).Lock()
	defer (*MU).Unlock()

	querySQL := "SELECT * FROM workers;"
	rows, err := DB.Query(querySQL)
	if err != nil {
		fmt.Println("Error querying data:", err)
		return
	}
	defer rows.Close()

	var ret []Worker
	for rows.Next() {
		var worker Worker
		err := rows.Scan(&worker.id, &worker.task_id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		ret = append(ret, worker)
	}

	s := `<table>
			<thead>
				<tr>
					<th>ID</th>
					<th>Status</th>
				</tr>
			</thead>
			<tbody id="taskTableBody">`
	for _, worker := range ret {
		s += "<tr>"
		s += "<td>" + fmt.Sprint(worker.id) + "</td>"
		if worker.task_id == -1 {
			s += "<td>Waiting for a task</td>"
		} else if worker.task_id == -2 {
			s += "<td>Has fallen</td>"
		} else {
			s += "<td>Pending task with id " + fmt.Sprint(worker.task_id) + "</td>"
		}
		s += "</tr>"
	}
	s += `	</tbody>
		</table>
		<button onclick="window.location.href='/addWorker?jwt_token=`+tokenString+`';">Запустить ещё один Worker</button>`

	data := struct {
		Title   string
		Header  string
		Content string
		Token string
	}{
		Title:   "Workers",
		Header:  "Workers active now:",
		Content: s,
		Token: tokenString,
	}

	tmpl, err := template.ParseFiles("../templates/template.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = tmpl.Execute(w, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func ValidTasks() {
	for {
		querySQL := "SELECT * FROM tasks WHERE status='pending';"
		rows, err := DB.Query(querySQL)
		if err != nil {
			fmt.Println("Error querying data:", err)
			return
		}
		defer rows.Close()

		var ret []Task
		for rows.Next() {
			var task Task
			err := rows.Scan(&task.ID, &task.Status, &task.Received, &task.Content, &task.Result, &task.Error, &task.User)
			if err != nil {
				fmt.Println("Error scanning data:", err)
				return
			}
			ret = append(ret, task)
		}

		for _, task := range ret {
			now, _ := strconv.Atoi(fmt.Sprintf("%d", time.Now().UnixNano()))
			task_time, _ := strconv.Atoi(task.ID)
			if time.Nanosecond*time.Duration(now-task_time) > time.Hour {
				(*MU).Lock()
				tx, err := DB.Begin()
				if err != nil {
					fmt.Printf(err.Error())
					return
				}

				querySQL = "UPDATE workers SET task_id=-2 WHERE task_id=?;"
				_, err = DB.Exec(querySQL, task.ID)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				}

				querySQL = "UPDATE tasks SET status='submitted' WHERE id=?;"
				_, err = DB.Exec(querySQL, task.ID)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				}

				if err := tx.Commit(); err != nil {
					fmt.Printf(err.Error())
				}
				(*MU).Unlock()
			}
		}

		time.Sleep(time.Minute * 5)
	}
}
