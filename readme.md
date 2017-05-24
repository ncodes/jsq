## JSQ - A mongoDB query parser for SQL databases

JSQ allows the use of similar mongoDB query expressions to find records in traditional SQL databases like Postgres, MySQL etc. 

### Installation
```
go get github.com/ncodes/jsq
```
### Example

```go
type Person struct {
    Name    string `json:"name" xorm:"name"`
    Age     int    `json:"age" xorm:"age"`
    RegNum  int64  `json:"reg_num" xorm:"reg_num"`
    Address string `json:"address" xorm:"address"`
}

jsq, err := NewJSQ("postgres", "ostgresql:-root@localhost:26257/mydb?sslmode=disable")

// Set the table to work on
jsq.SetTable(Person{}, false)

// Parse a query
err := jsq.Parse(`{
    "name": { 
        "$eq": "ben" 
    }
}`)
if err != nil {
    log.Fatalf("failed to parse: %s", err)
}

// Perform query
var r []Person
err = jsq.Find(&r)
if err != nil {
    log.Fatalf("failed to query: %s", err)
}
```

#### Supported Compare Operators
- $eq  - Equal
- $gt  - Greater Than
- $gte - Greater Than or Equal
- $lt  - Less Than
- $lte - Less Than or Equal
- $ne  - Not Equal
- $in  - In array
- $nin - Not In array
- $not - Not (negate)
- $sw  - Starts with
- $ew  - End with
- $ct  - Contains

### Logical Operators
- $and - Find records matching every expression in an array 
- $or  - Find records matching at least an expression in an array
- $nor - Find records that fail to match all expressions in an array

### Links

- See full operator usage and examples on the [mongoDB website](https://docs.mongodb.com/manual/reference/operator/query/)
- [Go Doc](https://godoc.org/github.com/ncodes/jsq)

### Todo:
- More query operators
- Support joins and aggregate functions

