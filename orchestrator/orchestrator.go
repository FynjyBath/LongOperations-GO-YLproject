package orchestrator

import (
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"text/template"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Task struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Received string `json:"received"`
	Content  string `json:"content"`
	Result   string `json:"result,omitempty"`
	Error    string `json:"error"`
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

	tx, err := DB.Begin()
	if err != nil {
		fmt.Fprintf(w, err.Error())
		return
	}

	mp := make(map[rune]int)
	mp['+'], err = strconv.Atoi(r.FormValue("number1"))
	mp['-'], err = strconv.Atoi(r.FormValue("number2"))
	mp['*'], err = strconv.Atoi(r.FormValue("number3"))
	mp['/'], err = strconv.Atoi(r.FormValue("number4"))

	for op, num := range mp {
		insertDataSQL := "UPDATE times SET time=? WHERE operation=?;"
		_, err = DB.Exec(insertDataSQL, num, op)
		insertDataSQL = "INSERT OR IGNORE INTO times VALUES (?, ?);"
		_, err = DB.Exec(insertDataSQL, op, num)
	}

	if err := tx.Commit(); err != nil {
		fmt.Fprintf(w, err.Error())
		return
	}

	http.Redirect(w, r, "/getTasks", http.StatusSeeOther)
}

func ChangeTimes(w http.ResponseWriter, r *http.Request) {
	(*MU).Lock()
	var op Operation
	mp := make(map[rune]int)
	querySQL := "SELECT * FROM times;"
	rows, err := DB.Query(querySQL)
	if err != nil {
		(*MU).Unlock()
		fmt.Printf(err.Error())
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
        <button type="submit">Изменить</button>
    </form>`, mp['+'], mp['-'], mp['*'], mp['/'])

	data := struct {
		Title   string
		Header  string
		Content string
	}{
		Title:   "Время выполнения",
		Header:  "Изменить время выполнения (в секундах):",
		Content: s,
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

	(*MU).Lock()
	defer (*MU).Unlock()

	taskID := generateID()

	tx, err := DB.Begin()
	if err != nil {
		fmt.Fprintf(w, err.Error())
		return
	}

	insertDataSQL := "INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?);"
	_, err = DB.Exec(insertDataSQL, taskID, "submitted", time.Now(), content, 0, "")
	if err != nil {
		fmt.Fprintf(w, err.Error())
		return
	}

	if err := tx.Commit(); err != nil {
		fmt.Fprintf(w, err.Error())
		return
	}

	fmt.Fprintf(w, "Expression '%s' submitted. Check status later with ID: %s\n", content, taskID)
}

func AddExpression(w http.ResponseWriter, r *http.Request) {
	s := `<form id="myForm">
				<label for="inputValue">Введите арифметическое выражение:</label><br>
				<input type="text" id="inputValue" name="inputValue"><br>
				<button type="button" onclick="sendValue()">Добавить задачу</button>
			</form>
			
			<script>
			function sendValue() {
				var inputValue = document.getElementById("inputValue").value;
				var xhttp = new XMLHttpRequest();
				xhttp.onreadystatechange = function() {
				if (this.readyState == 4 && this.status == 200) {
					console.log("Значение успешно отправлено на сервер");
					window.location.href = "/getTasks";
				}
				};
				xhttp.open("POST", "/receiveExpression", true);
				xhttp.setRequestHeader("Content-type", "application/x-www-form-urlencoded");
				xhttp.send("inputValue=" + inputValue.replace(/\+/g, "%2B"));
			}
			</script>`

	data := struct {
		Title   string
		Header  string
		Content string
	}{
		Title:   "Добавить задачу",
		Header:  "",
		Content: s,
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

func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func GetTasks(w http.ResponseWriter, r *http.Request) {
	(*MU).Lock()
	defer (*MU).Unlock()

	querySQL := "SELECT * FROM tasks;"
	rows, err := DB.Query(querySQL)
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
	}{
		Title:   "Задачи",
		Header:  "Архив задач:",
		Content: s,
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
		<button onclick="window.location.href='/addWorker';">Запустить ещё один Worker</button>`

	data := struct {
		Title   string
		Header  string
		Content string
	}{
		Title:   "Workers",
		Header:  "Workers active now:",
		Content: s,
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
			err := rows.Scan(&task.ID, &task.Status, &task.Received, &task.Content, &task.Result, &task.Error)
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
