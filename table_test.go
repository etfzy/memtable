package memtable

import (
	"fmt"
	"testing"
)

type UserFields struct {
	age  int
	name string
}

func TestTable(t *testing.T) {
	userTable := CreateTable[string, UserFields]()
	userTable.Insert("12", UserFields{
		age:  1,
		name: "12",
	})

	userTable.Insert("34", UserFields{
		age:  2,
		name: "34",
	})

	t.Run("SelectByMainKey", func(t *testing.T) {
		row, _ := userTable.SelectByMainKey("12")

		if row.name != "12" || row.age != 1 {
			t.Fatal("error ", row.name, row.age)
		}
	})

	t.Run("SelectByCond", func(t *testing.T) {
		rows := userTable.SelectByCondition(func(row UserFields) bool {
			if row.name == "12" {
				return true
			}
			return false
		})

		u := rows[0]

		if u.name != "12" || u.age != 1 {
			t.Fatal("error ", u.name, u.age)
		}
		fmt.Println(u.name, u.age)
	})

	t.Run("UpdateByMainKey", func(t *testing.T) {
		userTable.UpdateByMainkey("12", func(row UserFields) UserFields {
			if row.name == "12" {
				row.age = 3333
			}
			return row
		})

		row, _ := userTable.SelectByMainKey("12")

		if row.name != "12" || row.age != 3333 {
			t.Fatal("error ", row.name, row.age)
		}
		fmt.Println(row.name, row.age)
	})

	t.Run("UpdateByCond", func(t *testing.T) {
		userTable.UpdateByMainkey("12", func(row UserFields) UserFields {
			if row.name == "12" {
				row.age = 3333
			}
			return row
		})

		row, _ := userTable.SelectByMainKey("12")

		if row.name != "12" || row.age != 3333 {
			t.Fatal("error ", row.name, row.age)
		}
		fmt.Println(row.name, row.age)
	})

	t.Run("DelByMainKey", func(t *testing.T) {
		userTable.DelByMainkey("12")

		_, ok := userTable.SelectByMainKey("12")

		if ok {
			t.Fatal("error del by mainkey")
		}
	})

	t.Run("DelByCond", func(t *testing.T) {
		userTable.DelByCondition(func(row UserFields) bool {

			if row.name == "34" {
				return true
			}

			return false
		})

		_, ok := userTable.SelectByMainKey("34")

		if ok {
			t.Fatal("error del by cond")
		}
	})
}
