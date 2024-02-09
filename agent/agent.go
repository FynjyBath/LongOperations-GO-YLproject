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
}

type Operation struct {
	name rune
	t    string
}

var DB *sql.DB
var MU *sync.Mutex

func eval(expr string) (float64, error) {
	token.NewFileSet()
	tr, err := parser.ParseExpr(expr)
	if err != nil {
		return 0, err
	}

	mp := make(map[rune]int)
	(*MU).Lock()
	var op Operation
	querySQL := "SELECT * FROM times;"
	rows, err := DB.Query(querySQL)
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

	result, err := evalAST(tr, &mp)
	return result, err
}

func evalAST(expr ast.Expr, mp *map[rune]int) (float64, error) {
	switch expr := expr.(type) {
	case *ast.BinaryExpr:
		var wg sync.WaitGroup
		wg.Add(2)
		var err1, err2 error
		var left, right float64
		go func() {
			defer wg.Done()
			left, err1 = evalAST(expr.X, mp)
		}()
		go func() {
			defer wg.Done()
			right, err2 = evalAST(expr.Y, mp)
		}()
		wg.Wait()
		if err1 != nil {
			return 0, err1
		}
		if err2 != nil {
			return 0, err2
		}
		switch expr.Op {
		case token.ADD:
			time.Sleep(time.Second * time.Duration((*mp)['+']))
			return left + right, nil
		case token.SUB:
			time.Sleep(time.Second * time.Duration((*mp)['-']))
			return left - right, nil
		case token.MUL:
			time.Sleep(time.Second * time.Duration((*mp)['*']))
			return left * right, nil
		case token.QUO:
			time.Sleep(time.Second * time.Duration((*mp)['/']))
			if right == 0 {
				return 0, fmt.Errorf("division by zero")
			}
			return left / right, nil
		default:
			return 0, fmt.Errorf("unsupported operator: %s", expr.Op)
		}
	case *ast.BasicLit:
		if expr.Kind == token.INT {
			value, err := strconv.ParseFloat(expr.Value, 64)
			if err != nil {
				return 0, err
			}
			return value, nil
		}
	}
	return 0, fmt.Errorf("unsupported expression type")
}

func getTask() (Task, error) {
	tx, err := DB.Begin()
	if err != nil {
		return Task{}, err
	}

	var task Task
	querySQL := "SELECT * FROM tasks WHERE status='submitted';"
	err = DB.QueryRow(querySQL, 1).Scan(&task.ID, &task.Status, &task.Received, &task.Content, &task.Result, &task.Error)
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

func StartWorker(workerID int) {
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
			result, err = eval(task.Content)
		}()
		wg.Wait()

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
		if err != nil {
			task.Error = err.Error()
		}

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
	s, _ := strconv.Atoi(fmt.Sprintf("%d", time.Now().UnixNano()))
	StartWorker(s)
}
