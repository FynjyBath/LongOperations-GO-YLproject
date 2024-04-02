package agent

import (
	"database/sql"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"sync"
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
	User     string `json:"user"`
}

type Operation struct {
	name rune
	t    string
}

var DB *sql.DB
var MU *sync.Mutex

func eval(expr, login string) (float64, error) {
	tr, err := parser.ParseExpr(expr)
	if err != nil {
		return 0, err
	}

	mp := make(map[rune]int)
	(*MU).Lock()
	var op Operation
	querySQL := "SELECT operation, time FROM times WHERE user=?;"
	rows, err := DB.Query(querySQL, login)
	if err != nil {
		(*MU).Unlock()
		return 0, err
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

	fmt.Println(expr)

	result, err := evaluate(tr, &mp)
	return result, err
}

func evaluate(expr ast.Expr, mp *map[rune]int) (float64, error) {
	switch expr := expr.(type) {
	case *ast.BinaryExpr:
		left, err := evaluate(expr.X, mp)
		if err != nil {
			return 0, err
		}
		right, err := evaluate(expr.Y, mp)
		if err != nil {
			return 0, err
		}

		switch expr.Op {
		case token.ADD:
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Second * time.Duration((*mp)['+']))
			}()
			wg.Wait()
			return left + right, nil
		case token.SUB:
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Second * time.Duration((*mp)['-']))
			}()
			wg.Wait()
			return left - right, nil
		case token.MUL:
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Second * time.Duration((*mp)['*']))
			}()
			wg.Wait()
			return left * right, nil
		case token.QUO:
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Second * time.Duration((*mp)['/']))
			}()
			wg.Wait()
			if right == 0 {
				return 0, fmt.Errorf("division by zero")
			}
			return left / right, nil
		default:
			return 0, fmt.Errorf("unsupported operator: %v", expr.Op)
		}
	case *ast.BasicLit:
		value, err := strconv.ParseFloat(expr.Value, 64)
		if err != nil {
			return 0, err
		}
		return value, nil
	case *ast.ParenExpr:
		return evaluate(expr.X, mp)
	case *ast.UnaryExpr:
		value, err := evaluate(expr.X, mp)
		if err != nil {
			return 0, err
		}
		if expr.Op == token.SUB {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Second * time.Duration((*mp)['-']))
			}()
			wg.Wait()
			return -value, nil
		}
		return value, nil
	default:
		return 0, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func getTask() (Task, error) {
	tx, err := DB.Begin()
	if err != nil {
		return Task{}, err
	}

	var task Task
	querySQL := "SELECT * FROM tasks WHERE status='submitted';"
	err = DB.QueryRow(querySQL, 1).Scan(&task.ID, &task.Status, &task.Received, &task.Content, &task.Result, &task.Error, &task.User)
	if err != nil {
		return Task{}, err
	}

	querySQL = "UPDATE tasks SET status='pending' WHERE id=?;"
	_, err = DB.Exec(querySQL, task.ID)
	if err != nil {
		return Task{}, err
	}

	if err := tx.Commit(); err != nil {
		return Task{}, err
	}

	return task, nil
}

func StartWorker() {
	workerID, _ := strconv.Atoi(fmt.Sprintf("%d", time.Now().UnixNano()))
	(*MU).Lock()
	tx, err := DB.Begin()
	if err != nil {
		fmt.Printf(err.Error())
		return
	}
	querySQL := "INSERT INTO workers VALUES (?, -1);"
	_, err = DB.Exec(querySQL, workerID)
	if err != nil {
		fmt.Printf("Worker %d: Error set workers table: %v\n", workerID, err)
	}
	if err := tx.Commit(); err != nil {
		fmt.Printf(err.Error())
	}
	(*MU).Unlock()
	for {
		(*MU).Lock()
		task, err := getTask()
		(*MU).Unlock()
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		(*MU).Lock()
		tx, err := DB.Begin()
		if err != nil {
			fmt.Printf(err.Error())
			return
		}
		querySQL := "UPDATE workers SET task_id=? WHERE id=?;"
		_, err = DB.Exec(querySQL, task.ID, workerID)
		if err != nil {
			fmt.Printf("Worker %d: Error set workers table: %v\n", workerID, err)
		}
		if err := tx.Commit(); err != nil {
			fmt.Printf(err.Error())
		}
		(*MU).Unlock()

		var result float64
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err = eval(task.Content, task.User)
		}()
		wg.Wait()
		if err != nil {
			task.Error = err.Error()
		}

		(*MU).Lock()
		tx, err = DB.Begin()
		if err != nil {
			fmt.Printf(err.Error())
			return
		}
		querySQL = "UPDATE workers SET task_id=-1 WHERE id=?;"
		_, err = DB.Exec(querySQL, workerID)
		if err != nil {
			fmt.Printf("Worker %d: Error set workers table: %v\n", workerID, err)
		}
		if err := tx.Commit(); err != nil {
			fmt.Printf(err.Error())
		}
		(*MU).Unlock()

		task.Result = strconv.FormatFloat(result, 'f', -1, 64)

		(*MU).Lock()
		tx, err = DB.Begin()
		if err != nil {
			fmt.Printf(err.Error())
			return
		}
		fmt.Printf(task.Result)
		querySQL = "UPDATE tasks SET status='completed', result=?, error=? WHERE id=?;"
		_, err = DB.Exec(querySQL, task.Result, task.Error, task.ID)
		if err != nil {
			fmt.Printf("Worker %d: Error sending result: %v\n", workerID, err)
		}
		if err := tx.Commit(); err != nil {
			fmt.Printf(err.Error())
		}
		(*MU).Unlock()
	}
}

func main() {
	StartWorker()
}
