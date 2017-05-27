package jsq

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/ellcrys/util"
	"github.com/go-xorm/builder"
	"github.com/go-xorm/xorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	. "github.com/smartystreets/goconvey/convey"
)

var testDB *sql.DB

var dbName = "test_" + strings.ToLower(util.RandString(5))
var conStr = "postgresql://root@localhost:26257?sslmode=disable"
var conStrWithDB = "postgresql://root@localhost:26257/" + dbName + "?sslmode=disable"

func init() {
	var err error
	testDB, err = sql.Open("postgres", conStr)
	if err != nil {
		panic(fmt.Errorf("failed to connect to database: %s", err))
	}
}

func createDb(t *testing.T) error {
	_, err := testDB.Query(fmt.Sprintf("CREATE DATABASE %s;", dbName))
	return err
}

func dropDB(t *testing.T) error {
	_, err := testDB.Query(fmt.Sprintf("DROP DATABASE %s;", dbName))
	return err
}

func clearTable(db *sql.DB, tables ...string) error {
	_, err := db.Exec("TRUNCATE " + strings.Join(tables, ","))
	if err != nil {
		return err
	}
	return nil
}

type Person struct {
	Name      string `json:"name" xorm:"name"`
	Age       int    `json:"age" xorm:"age"`
	RegNum    int64  `json:"reg_num" xorm:"reg_num"`
	Address   string `json:"address" xorm:"address"`
	Timestamp int64  `json:"timestamp" xorm:"timestamp;NULL"`
}

func TestJSQ(t *testing.T) {
	if err := createDb(t); err != nil {
		t.Fatalf("failed to create test database. %s", err)
	}
	defer dropDB(t)

	engine, err := xorm.NewEngine("postgres", conStrWithDB)
	if err != nil {
		t.Fatalf("failed to connect to database. %s", err)
	}

	jsq := NewJSQ(nil)

	engine.CreateTables(Person{})

	Convey("JSQ", t, func() {

		Convey(".Parse", func() {
			Convey("Should return error if json is malformed", func() {
				json := `{""}`
				err := jsq.Parse(json)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "malformed json")
			})
		})

		Convey(".isValidField", func() {
			Convey("when no field is whitelisted, all fields are allowed", func() {
				So(jsq.isValidField("name"), ShouldEqual, true)
				So(jsq.isValidField("unknown"), ShouldEqual, true)
			})

			Convey("when fields are whitelisted, unknown fields are invalid", func() {
				jsq := NewJSQ([]string{"name"})
				So(jsq.isValidField("name"), ShouldEqual, true)
				So(jsq.isValidField("unknown"), ShouldEqual, false)
			})
		})

		Convey(".isValidOperator", func() {
			operators := []string{"op1", "op2"}
			So(jsq.isValidOperator("op1", operators), ShouldEqual, true)
			So(jsq.isValidOperator("op2", operators), ShouldEqual, true)
			So(jsq.isValidOperator("op3", operators), ShouldEqual, false)
		})

		Convey(".getBuilder", func() {
			b := jsq.getBuilder(parserCtx{})
			So(b, ShouldResemble, jsq.b)
			b = jsq.getBuilder(parserCtx{b: &builder.Builder{}})
			So(b, ShouldNotResemble, jsq.b)
		})

		Convey(".validateCompareOperators", func() {
			Convey("Should return error if parameter is not a map", func() {
				So(jsq.validateCompareOperators("invalid"), ShouldResemble, fmt.Errorf("expected a map type"))
			})

			Convey("Should return error if map contains an unknown compare operator", func() {
				err := jsq.validateCompareOperators(map[string]interface{}{"$eq": ""})
				So(err, ShouldBeNil)
				err = jsq.validateCompareOperators(map[string]interface{}{"$unknown": ""})
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "bad value. unknown operator: $unknown")
			})
		})

		Convey(".isEmptyBuilder", func() {
			So(jsq.isEmptyBuilder(), ShouldEqual, true)
			jsq.b = &builder.Builder{}
			jsq.b.And(builder.Expr("stuff = stuff"))
			So(jsq.isEmptyBuilder(), ShouldEqual, false)
			jsq.b = &builder.Builder{}
		})

		Convey("Test with samples", func() {

			persons := []interface{}{
				Person{Name: "ken", Age: 20, RegNum: 12345, Address: "street 1"},
				Person{Name: "ben", Age: 21, RegNum: 12346, Address: "street 2"},
				Person{Name: "zen", Age: 22, RegNum: 12347, Address: "street 3"},
				Person{Name: "gen", Age: 23, RegNum: 12348, Address: "street 4"},
			}
			affected, err := engine.Insert(persons...)
			So(affected, ShouldEqual, 4)
			So(err, ShouldBeNil)

			Convey("Should return all records if query is empty", func() {
				err := jsq.Parse(`{}`)
				So(err, ShouldBeNil)
				var r []Person
				sql, args, err := jsq.ToSQL()
				So(err, ShouldBeNil)
				err = engine.Table(Person{}).Where(sql, args).Find(&r)
				So(err, ShouldBeNil)
				So(len(r), ShouldEqual, 4)
			})

			Convey("equality ($eq)", func() {
				Convey("Should get all persons with name=ben without the $eq operator", func() {
					err := jsq.Parse(`{"name": "ben"}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 1)
					So(r[0], ShouldResemble, persons[1])
				})

				Convey("Should all persons with name=ben with the $eq operator", func() {
					err := jsq.Parse(`{"name": { "$eq": "ben" }}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 1)
					So(r[0], ShouldResemble, persons[1])
				})

				Reset(func() {
					err := clearTable(engine.DB().DB, "person")
					So(err, ShouldBeNil)
				})
			})

			Convey("$gt", func() {
				Convey("Should get all persons with age greater than 21 ", func() {
					err := jsq.Parse(`{"age": { "$gt": 21 }}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 2)
					So(r[0], ShouldResemble, persons[2])
					So(r[1], ShouldResemble, persons[3])
				})
			})

			Convey("$gte", func() {
				Convey("Should get all persons with age greater than or equal to 21 ", func() {
					err := jsq.Parse(`{"age": { "$gte": 21 }}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 3)
					So(r[0], ShouldResemble, persons[1])
					So(r[1], ShouldResemble, persons[2])
					So(r[2], ShouldResemble, persons[3])
				})
			})

			Convey("$lt", func() {
				Convey("Should get all persons with age less than 21 ", func() {
					err := jsq.Parse(`{"age": { "$lt": 21 }}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 1)
					So(r[0], ShouldResemble, persons[0])
				})
			})

			Convey("$lte", func() {
				Convey("Should get all persons with age less than or equal to 21 ", func() {
					err := jsq.Parse(`{"age": { "$lte": 21 }}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 2)
					So(r[0], ShouldResemble, persons[0])
					So(r[1], ShouldResemble, persons[1])
				})
			})

			Convey("$ne", func() {
				Convey("Should get all persons with age not equal to 21 ", func() {
					err := jsq.Parse(`{"age": { "$ne": 21 }}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 3)
					So(r, ShouldNotContain, persons[1])
				})
			})

			Convey("$in", func() {
				Convey("Should get all persons with age in [21,23] ", func() {
					err := jsq.Parse(`{"age": { "$in": [21, 23] }}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 2)
					So(r, ShouldContain, persons[1])
					So(r, ShouldContain, persons[3])
				})
			})

			Convey("$nin", func() {
				Convey("Should get all persons with age not in [21,23] ", func() {
					err := jsq.Parse(`{"age": { "$nin": [21, 23] }}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 2)
					So(r, ShouldNotContain, persons[1])
					So(r, ShouldNotContain, persons[3])
				})
			})

			Convey("$not", func() {

				Convey("Should return error when used as a top level operator", func() {
					err := jsq.Parse(`{"$not": { }}`)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldEqual, "unknown top level operator: $not")
				})

				Convey("Should return error when assigned a value that is not a map type", func() {
					err := jsq.Parse(`{"name": { "$not": "abc"}}`)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldEqual, "field 'name': '$not' operator supports only map type")
				})

				Convey("Should return all persons with name not equal to ben", func() {
					err := jsq.Parse(`{"name": { "$not": { "$eq": "ben" }}}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 3)
					So(r, ShouldNotContain, persons[1])
				})

				Reset(func() {
					err := clearTable(engine.DB().DB, "person")
					So(err, ShouldBeNil)
				})
			})

			Convey("$sw", func() {
				Convey("Should get all persons with name starting with 'be'", func() {
					err := jsq.Parse(`{"name": { "$sw": "be" }}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 1)
					So(r, ShouldContain, persons[1])
				})
			})

			Convey("$ew", func() {
				Convey("Should get all persons with name ending with 'en'", func() {
					err := jsq.Parse(`{"name": { "$ew": "en" }}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 4)
				})
			})

			Convey("$ct", func() {
				Convey("Should get all persons with address containing the substring 'reet'", func() {
					err := jsq.Parse(`{"address": { "$ct": "reet" }}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 4)
				})
			})

			Convey("Complex queries", func() {

				err := clearTable(engine.DB().DB, "person")
				So(err, ShouldBeNil)

				persons := []interface{}{
					Person{Name: "ben", Age: 21, RegNum: 3000, Address: "street 2"},
					Person{Name: "ken", Age: 20, RegNum: 12345, Address: "street 1"},
					Person{Name: "ben", Age: 21, RegNum: 12346, Address: "street 2"},
					Person{Name: "zen", Age: 22, RegNum: 12347, Address: "street 3"},
					Person{Name: "gen", Age: 23, RegNum: 12348, Address: "street 4"},
				}
				affected, err := engine.Insert(persons...)
				So(affected, ShouldEqual, 5)
				So(err, ShouldBeNil)

				Convey("Should get all persons name equal to ben, age equal to 21 and reg_num equal 3000", func() {
					err := jsq.Parse(`{
								"name": "ben",
								"age": 21,
								"reg_num": 3000
							}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 1)
					So(r[0], ShouldResemble, persons[0])
				})

				Convey("Should get all persons with name equal to ben, age equal to 21 and reg_num equal 3000 (use $eq operator)", func() {
					err := jsq.Parse(`{
								"name": { "$eq": "ben" },
								"age": 21,
								"reg_num": { "$eq": 3000 }
							}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 1)
					So(r[0], ShouldResemble, persons[0])
				})

				Convey("Should get all persons with name not equal to ben, age not equal to 21", func() {
					err := jsq.Parse(`{
								"name": { "$not": { "$eq": "ben" }},
								"age": { "$not": { "$eq": 21 }}
							}`)
					So(err, ShouldBeNil)
					var r []Person
					sql, args, err := jsq.ToSQL()
					So(err, ShouldBeNil)
					err = engine.Table(Person{}).Where(sql, args...).Find(&r)
					So(err, ShouldBeNil)
					So(len(r), ShouldEqual, 3)
					So(r, ShouldNotContain, persons[0])
					So(r, ShouldNotContain, persons[2])
				})

				Convey("logical operators", func() {

					err := clearTable(engine.DB().DB, "person")
					So(err, ShouldBeNil)

					persons := []interface{}{
						Person{Name: "ben", Age: 21, RegNum: 3000, Address: "street 2"},
						Person{Name: "ken", Age: 20, RegNum: 12345, Address: "street 1"},
						Person{Name: "ben", Age: 21, RegNum: 12346, Address: "street 2"},
						Person{Name: "zen", Age: 22, RegNum: 12347, Address: "street 3"},
						Person{Name: "gen", Age: 23, RegNum: 12348, Address: "street 4"},
					}
					affected, err := engine.Insert(persons...)
					So(affected, ShouldEqual, 5)
					So(err, ShouldBeNil)

					Convey("$and", func() {
						Convey("Should return error if value type is not an array", func() {
							err := jsq.Parse(`{
								"$and": "invalid type"
							}`)
							So(err, ShouldNotBeNil)
							So(err.Error(), ShouldEqual, "field '$and': operator supports only array type")
						})

						Convey("Should get all persons with name = ben and age = 21", func() {
							err := jsq.Parse(`{
								"$and": [{ "name": "ben" }, { "age": 21 }]
							}`)
							So(err, ShouldBeNil)
							var r []Person
							sql, args, err := jsq.ToSQL()
							So(err, ShouldBeNil)
							err = engine.Table(Person{}).Where(sql, args...).Find(&r)
							So(err, ShouldBeNil)
							So(len(r), ShouldEqual, 2)
							So(r, ShouldContain, persons[0])
							So(r, ShouldContain, persons[2])
						})

						Convey("Should fail get a person with name = ben and age = 50", func() {
							err := jsq.Parse(`{
								"$and": [{ "name": "ben" }, { "age": 50 }]
							}`)
							So(err, ShouldBeNil)
							var r []Person
							sql, args, err := jsq.ToSQL()
							So(err, ShouldBeNil)
							err = engine.Table(Person{}).Where(sql, args...).Find(&r)
							So(err, ShouldBeNil)
							So(len(r), ShouldEqual, 0)
						})

						Convey("Should return error when used in query/compare context", func() {
							err := jsq.Parse(`{
								"name": { "$and": []}
							}`)
							So(err, ShouldNotBeNil)
							So(err.Error(), ShouldEqual, "field 'name': bad value. unknown operator: $and")
						})
					})

					Convey("$or", func() {
						Convey("Should get all objects with name = ken or name = gen", func() {
							err := jsq.Parse(`{
								"$or": [{ "name": "ken" }, { "name": "gen" }, { "name": "fen" }]
							}`)
							So(err, ShouldBeNil)
							var r []Person
							sql, args, err := jsq.ToSQL()
							So(err, ShouldBeNil)
							err = engine.Table(Person{}).Where(sql, args...).Find(&r)
							So(err, ShouldBeNil)
							So(len(r), ShouldEqual, 2)
							So(r, ShouldContain, persons[1])
							So(r, ShouldContain, persons[4])
						})
					})

					Convey("$nor", func() {
						Convey("Should get all objects with name != ken or name != ben or name != zen", func() {
							err := jsq.Parse(`{
											"$nor": [{ "name": "ken" }, { "name": "ben" }, { "name": "zen" }]
										}`)
							So(err, ShouldBeNil)
							var r []Person
							sql, args, err := jsq.ToSQL()
							So(err, ShouldBeNil)
							err = engine.Table(Person{}).Where(sql, args...).Find(&r)
							So(err, ShouldBeNil)
							So(len(r), ShouldEqual, 1)
							So(r, ShouldContain, persons[4])
						})
					})

					Reset(func() {
						err := clearTable(engine.DB().DB, "person")
						So(err, ShouldBeNil)
					})
				})

				Reset(func() {
					err := clearTable(engine.DB().DB, "person")
					So(err, ShouldBeNil)
				})
			})

			Reset(func() {
				err := clearTable(engine.DB().DB, "person")
				So(err, ShouldBeNil)
			})
		})
	})
}
