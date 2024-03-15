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
	userTable := CreateTable()
	userTable.Insert("12", UserFields{
		age:  1,
		name: "12",
	})

	userTable.Insert("34", UserFields{
		age:  2,
		name: "34",
	})

	t.Run("SelectByMainKey", func(t *testing.T) {
		row := userTable.SelectByMainKey("12")

		u := row.(UserFields)

		if u.name != "12" || u.age != 1 {
			t.Fatal("error ", u.name, u.age)
		}
	})

	t.Run("SelectByCond", func(t *testing.T) {
		row := userTable.SelectByCondition(func(row any) bool {
			u := row.(UserFields)
			if u.name == "12" {
				return true
			}
			return false
		})

		u := row[0].(UserFields)

		if u.name != "12" || u.age != 1 {
			t.Fatal("error ", u.name, u.age)
		}
		fmt.Println(u.name, u.age)
	})

	t.Run("UpdateByMainKey", func(t *testing.T) {
		userTable.UpdateByMainkey("12", func(row any) any {
			u := row.(UserFields)
			if u.name == "12" {
				u.age = 3333
			}
			return u
		})

		row := userTable.SelectByMainKey("12")

		u := row.(UserFields)

		if u.name != "12" || u.age != 3333 {
			t.Fatal("error ", u.name, u.age)
		}
		fmt.Println(u.name, u.age)
	})

	t.Run("UpdateByCond", func(t *testing.T) {
		userTable.UpdateByMainkey("12", func(row any) any {
			u := row.(UserFields)
			if u.name == "12" {
				u.age = 3333
			}
			return u
		})

		row := userTable.SelectByMainKey("12")

		u := row.(UserFields)

		if u.name != "12" || u.age != 3333 {
			t.Fatal("error ", u.name, u.age)
		}
		fmt.Println(u.name, u.age)
	})

	t.Run("DelByMainKey", func(t *testing.T) {
		userTable.DelByMainkey("12")

		row := userTable.SelectByMainKey("12")

		if row != nil {
			t.Fatal("error del by mainkey")
		}
	})

	t.Run("DelByCond", func(t *testing.T) {
		userTable.DelByCondition(func(row any) bool {
			u := row.(UserFields)
			if u.name == "34" {
				return true
			}

			return false
		})

		row := userTable.SelectByMainKey("34")

		if row != nil {
			t.Fatal("error del by cond")
		}
	})
}
